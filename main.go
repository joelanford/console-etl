package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	catalogdv1alpha1 "github.com/operator-framework/catalogd/api/core/v1alpha1"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
)

func main() {
	var (
		kubeconfig string
		namespace  string
		service    string
		localPort  uint
		destPort   uint
		cacheRoot  string
	)

	if home := homedir.HomeDir(); home != "" {
		flag.StringVar(&kubeconfig, "kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.StringVar(&namespace, "namespace", "olmv1-system", "namespace of the service")
	flag.StringVar(&service, "service", "catalogd-catalogserver", "service to port-forward")
	flag.UintVar(&localPort, "local-port", 0, "local port to listen on")
	flag.UintVar(&destPort, "dest-port", 443, "destination port to forward")

	flag.StringVar(&cacheRoot, "cache-root", "cache", "root directory to cache catalog data")
	flag.Parse()

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("failed to build kubeconfig: %v", err)
	}

	spf, err := openServicePortForward(context.Background(), cfg, uint16(localPort), uint16(destPort), types.NamespacedName{Namespace: namespace, Name: service})
	if err != nil {
		log.Fatalf("failed to open port forward: %v", err)
	}
	log.Printf("port forward opened on %d", spf.localPort)
	defer spf.Close()

	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("failed to create dynamic client: %v", err)
	}

	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	cc := newCachingClient(cacheRoot, fmt.Sprintf("https://localhost:%d", spf.localPort), &http.Client{
		Transport: httpTransport,
	})

	// Standard generic endpoints
	http.Handle("GET /{resource}", listResourceInstancesHandler(dynamicClient))
	http.Handle("GET /{resource}/{catalogName}", getCatalogHandler(dynamicClient))
	http.Handle("GET /{resource}/{catalogName}/packages", listPackagesHandler(dynamicClient, cc))
	http.Handle("GET /{resource}/{catalogName}/packages/{packageName}", listPackageSchemasHandler(dynamicClient, cc))
	http.Handle("GET /{resource}/{catalogName}/packages/{packageName}/{schema}", listObjectsHandler(dynamicClient, cc))
	http.Handle("GET /{resource}/{catalogName}/packages/{packageName}/{schema}/{objectName}", getObjectHandler(dynamicClient, cc))

	// Custom endpoints
	http.Handle("GET /{resource}/{catalogName}/packages/{packageName}/icon", getPackageIconHandler(dynamicClient, cc))

	log.Println("Listening on :8080")
	_ = http.ListenAndServe(":8080", nil)
}

func listResourceInstancesHandler(dynamicClient dynamic.Interface) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uList, err := dynamicClient.Resource(schema.GroupVersionResource{Group: "catalogd.operatorframework.io", Version: "v1alpha1", Resource: r.PathValue("resource")}).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(uList); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func getCatalogHandler(dynamicClient dynamic.Interface) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cat, err := getClusterCatalog(r.Context(), dynamicClient, r.PathValue("resource"), r.PathValue("catalogName"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(cat); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func listPackagesHandler(dynamicClient dynamic.Interface, cc *cachingClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cat, err := getClusterCatalog(r.Context(), dynamicClient, r.PathValue("resource"), r.PathValue("catalogName"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if cat.Status.Phase != catalogdv1alpha1.PhaseUnpacked {
			http.Error(w, "catalog not unpacked", http.StatusServiceUnavailable)
			return
		}

		fsys, err := cc.getCatalogFS(context.Background(), cat)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		entries, err := fs.ReadDir(fsys, ".")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		packageNames := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.IsDir() {
				packageNames = append(packageNames, entry.Name())
			}
		}
		sort.Strings(packageNames)

		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(packageNames); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func listPackageSchemasHandler(dynamicClient dynamic.Interface, cc *cachingClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cat, err := getClusterCatalog(r.Context(), dynamicClient, r.PathValue("resource"), r.PathValue("catalogName"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if cat.Status.Phase != catalogdv1alpha1.PhaseUnpacked {
			http.Error(w, "catalog not unpacked", http.StatusServiceUnavailable)
			return
		}

		fsys, err := cc.getCatalogFS(context.Background(), cat)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		entries, err := fs.ReadDir(fsys, r.PathValue("packageName"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		schemaNames := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.IsDir() {
				schemaNames = append(schemaNames, entry.Name())
			}
		}
		sort.Strings(schemaNames)
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(schemaNames); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func listObjectsHandler(dynamicClient dynamic.Interface, cc *cachingClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cat, err := getClusterCatalog(r.Context(), dynamicClient, r.PathValue("resource"), r.PathValue("catalogName"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if cat.Status.Phase != catalogdv1alpha1.PhaseUnpacked {
			http.Error(w, "catalog not unpacked", http.StatusServiceUnavailable)
			return
		}

		fsys, err := cc.getCatalogFS(context.Background(), cat)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		entries, err := fs.ReadDir(fsys, filepath.Join(r.PathValue("packageName"), r.PathValue("schema")))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		objectNames := make([]string, 0, len(entries))
		for _, entry := range entries {
			if !entry.IsDir() {
				objectNames = append(objectNames, strings.TrimSuffix(entry.Name(), ".json"))
			}
		}
		sort.Strings(objectNames)

		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(objectNames); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func getObjectHandler(dynamicClient dynamic.Interface, cc *cachingClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cat, err := getClusterCatalog(r.Context(), dynamicClient, r.PathValue("resource"), r.PathValue("catalogName"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if cat.Status.Phase != catalogdv1alpha1.PhaseUnpacked {
			http.Error(w, "catalog not unpacked", http.StatusServiceUnavailable)
			return
		}

		fsys, err := cc.getCatalogFS(context.Background(), cat)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		http.ServeFileFS(w, r, fsys, filepath.Join(r.PathValue("packageName"), r.PathValue("schema"), r.PathValue("objectName")+".json"))
	})
}

func getPackageIconHandler(dynamicClient dynamic.Interface, cc *cachingClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cat, err := getClusterCatalog(r.Context(), dynamicClient, r.PathValue("resource"), r.PathValue("catalogName"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if cat.Status.Phase != catalogdv1alpha1.PhaseUnpacked {
			http.Error(w, "catalog not unpacked", http.StatusServiceUnavailable)
			return
		}

		fsys, err := cc.getCatalogFS(context.Background(), cat)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// TODO: This assumes that the only place to get the icon is from the "olm.package"
		//   blob. Today, this _IS_ the only place to get the icon, but in the future, we may
		//   define a separate `olm.icon` or `olm.package.v2` specification where an icon might
		//   be found.
		pkgData, err := fs.ReadFile(fsys, filepath.Join(r.PathValue("packageName"), declcfg.SchemaPackage, r.PathValue("packageName")+".json"))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				http.Error(w, "package not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var pkg declcfg.Package
		if err := json.Unmarshal(pkgData, &pkg); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if pkg.Icon == nil {
			http.Error(w, "icon not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", pkg.Icon.MediaType)
		if _, err := w.Write(pkg.Icon.Data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func getClusterCatalog(ctx context.Context, dynamicClient dynamic.Interface, resource, catalogName string) (*catalogdv1alpha1.ClusterCatalog, error) {
	u, err := dynamicClient.Resource(schema.GroupVersionResource{Group: "catalogd.operatorframework.io", Version: "v1alpha1", Resource: resource}).Get(ctx, catalogName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	var catalog catalogdv1alpha1.ClusterCatalog
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &catalog); err != nil {
		return nil, err
	}
	return &catalog, nil
}
