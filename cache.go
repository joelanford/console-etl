package main

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"

	catalogdv1alpha1 "github.com/operator-framework/catalogd/api/core/v1alpha1"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
)

type cachingClient struct {
	baseURL    string
	cacheDir   string
	httpClient *http.Client
	lru        *expirable.LRU[string, string]
}

func newCachingClient(cacheDir string, baseURL string, httpClient *http.Client) *cachingClient {
	lru := expirable.NewLRU(100, func(_ string, value string) {
		os.RemoveAll(value)
	}, time.Hour*24)
	return &cachingClient{
		baseURL:    baseURL,
		cacheDir:   cacheDir,
		httpClient: httpClient,
		lru:        lru,
	}
}

func (c *cachingClient) getCatalogFS(ctx context.Context, clusterCatalog *catalogdv1alpha1.ClusterCatalog) (fs.FS, error) {
	catalogPath := filepath.Join(c.cacheDir, "clustercatalogs", clusterCatalog.GetName())
	c.lru.Add(clusterCatalog.GetName(), catalogPath)

	activeSymlinkPath := filepath.Join(catalogPath, "active")
	catalogStat, err := os.Stat(activeSymlinkPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/catalogs/"+clusterCatalog.GetName()+"/all.json", nil)
	if err != nil {
		return nil, err
	}

	if catalogStat != nil && catalogStat.IsDir() {
		modTime := catalogStat.ModTime().UTC().Format(http.TimeFormat)
		req.Header.Set("If-Modified-Since", modTime)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return os.DirFS(activeSymlinkPath), nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, err
	}

	modTime, err := http.ParseTime(resp.Header.Get("Last-Modified"))
	if err != nil {
		return nil, err
	}
	realCatalogPath := filepath.Join(catalogPath, modTime.UTC().Format("20060102_150405"))
	if err != nil {
		return nil, err
	}
	if err := writeCatalog(realCatalogPath, resp.Body); err != nil {
		return nil, err
	}
	if err := os.Chtimes(realCatalogPath, modTime, modTime); err != nil {
		return nil, err
	}
	nextSymlinkPath := filepath.Join(catalogPath, "next")
	if err := os.Symlink(filepath.Base(realCatalogPath), filepath.Join(catalogPath, "next")); err != nil {
		return nil, err
	}
	if err := os.Rename(nextSymlinkPath, activeSymlinkPath); err != nil {
		return nil, err
	}
	return os.DirFS(activeSymlinkPath), nil
}

func writeCatalog(catalogDir string, r io.Reader) error {
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
