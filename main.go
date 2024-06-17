package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/operator-framework/operator-registry/alpha/declcfg"
)

func main() {
	var (
		cacheRoot   string
		catalogKind string
		catalogKey  string
	)

	flag.StringVar(&catalogKind, "resource", "clustercatalogs", "resource of the catalog")
	flag.StringVar(&catalogKey, "key", "", "key of the catalog")
	flag.Parse()

	catalogDir := filepath.Join(cacheRoot, catalogKind, catalogKey)
	if err := writeCatalog(context.Background(), catalogDir, os.Stdin); err != nil {
		log.Fatal(err)
	}

	// Standard generic endpoints
	http.Handle("GET /{resource}", listResourceInstancesHandler(cacheRoot))
	http.Handle("GET /{resource}/{catalogName}", listPackagesHandler(cacheRoot))
	http.Handle("GET /{resource}/{catalogName}/{packageName}", listPackageSchemasHandler(cacheRoot))
	http.Handle("GET /{resource}/{catalogName}/{packageName}/{schema}", listObjectsHandler(cacheRoot))
	http.Handle("GET /{resource}/{catalogName}/{packageName}/{schema}/{objectName}", getObjectHandler(cacheRoot))

	// Custom endpoints
	http.Handle("GET /{resource}/{catalogName}/{packageName}/icon", getPackageIconHandler(cacheRoot))

	log.Println("Listening on :8080")
	_ = http.ListenAndServe(":8080", nil)
}

func writeCatalog(_ context.Context, catalogDir string, r io.Reader) error {
	if err := declcfg.WalkMetasReader(r, func(m *declcfg.Meta, err error) error {
		if err != nil {
			return err
		}
		packageName := m.Package
		if m.Schema == declcfg.SchemaPackage {
			packageName = m.Name
		}
		if packageName == "" {
			packageName = "__global"
		}
		path := filepath.Join(catalogDir, packageName, m.Schema, m.Name+".json")
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(path, m.Blob, 0644); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func listResourceInstancesHandler(cacheRoot string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		catalogsDir := filepath.Join(cacheRoot, r.PathValue("resource"))
		entries, err := os.ReadDir(catalogsDir)
		if err != nil {
			if os.IsNotExist(err) {
				http.Error(w, "resource not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		catalogNames := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.IsDir() {
				catalogNames = append(catalogNames, entry.Name())
			}
		}
		sort.Strings(catalogNames)

		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(catalogNames); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func listPackagesHandler(cacheRoot string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		catalogDir := filepath.Join(cacheRoot, r.PathValue("resource"), r.PathValue("catalogName"))

		entries, err := os.ReadDir(catalogDir)
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

func listPackageSchemasHandler(cacheRoot string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		packageDir := filepath.Join(cacheRoot,
			r.PathValue("resource"),
			r.PathValue("catalogName"),
			r.PathValue("packageName"))

		entries, err := os.ReadDir(packageDir)
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

func listObjectsHandler(cacheRoot string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		objectsDir := filepath.Join(cacheRoot,
			r.PathValue("resource"),
			r.PathValue("catalogName"),
			r.PathValue("packageName"),
			r.PathValue("schema"))

		entries, err := os.ReadDir(objectsDir)
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

func getObjectHandler(cacheRoot string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(cacheRoot,
			r.PathValue("resource"),
			r.PathValue("catalogName"),
			r.PathValue("packageName"),
			r.PathValue("schema"),
			r.PathValue("objectName")+".json"))
	})
}

func getPackageIconHandler(cacheRoot string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// TODO: This assumes that the only place to get the icon is from the "olm.package"
		//   blob. Today, this _IS_ the only place to get the icon, but in the future, we may
		//   define a separate `olm.icon` or `olm.package.v2` specification where an icon might
		//   be found.
		pkgData, err := os.ReadFile(filepath.Join(cacheRoot,
			r.PathValue("resource"),
			r.PathValue("catalogName"),
			r.PathValue("packageName"),
			declcfg.SchemaPackage,
			r.PathValue("packageName")+".json"))
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
