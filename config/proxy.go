package config

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"

	"github.com/buchgr/bazel-remote/v2/cache/azblobproxy"
	"github.com/buchgr/bazel-remote/v2/cache/gcsproxy"
	"github.com/buchgr/bazel-remote/v2/cache/httpproxy"
	"github.com/buchgr/bazel-remote/v2/cache/peerproxy"
	"github.com/buchgr/bazel-remote/v2/cache/s3proxy"
	"github.com/minio/minio-go/v7"
)

func (c *Config) setProxy() error {
	if c.GoogleCloudStorage != nil {
		proxyCache, err := gcsproxy.New(c.GoogleCloudStorage.Bucket,
			c.GoogleCloudStorage.UseDefaultCredentials, c.GoogleCloudStorage.JSONCredentialsFile,
			c.StorageMode, c.AccessLogger, c.ErrorLogger, c.NumUploaders, c.MaxQueuedUploads)
		if err != nil {
			return err
		}

		c.ProxyBackend = proxyCache
	}

	if c.ProxyBackend == nil && c.HTTPBackend != nil {
		httpClient := &http.Client{}
		var baseURL *url.URL
		baseURL, err := url.Parse(c.HTTPBackend.BaseURL)
		if err != nil {
			return err
		}
		proxyCache, err := httpproxy.New(baseURL, c.StorageMode,
			httpClient, c.AccessLogger, c.ErrorLogger, c.NumUploaders, c.MaxQueuedUploads)
		if err != nil {
			return err
		}

		c.ProxyBackend = proxyCache
	}

	if c.ProxyBackend == nil && c.S3CloudStorage != nil {
		creds, err := c.S3CloudStorage.GetCredentials()
		if err != nil {
			return err
		}

		bucketLookupType, err := parseBucketLookupType(c.S3CloudStorage.BucketLookupType)
		if err != nil {
			return err
		}
		c.ProxyBackend = s3proxy.New(
			c.S3CloudStorage.Endpoint,
			c.S3CloudStorage.Bucket,
			bucketLookupType,
			c.S3CloudStorage.Prefix,
			creds,
			c.S3CloudStorage.DisableSSL,
			c.S3CloudStorage.UpdateTimestamps,
			c.S3CloudStorage.Region,
			c.StorageMode, c.AccessLogger, c.ErrorLogger, c.NumUploaders, c.MaxQueuedUploads)
	}

	if c.ProxyBackend == nil && c.AzBlobConfig != nil {
		creds, err := c.AzBlobConfig.GetCredentials()
		if err != nil {
			return err
		}

		c.ProxyBackend = azblobproxy.New(
			c.AzBlobConfig.StorageAccount,
			c.AzBlobConfig.ContainerName,
			c.AzBlobConfig.Prefix,
			creds,
			c.AzBlobConfig.SharedKey,
			c.AzBlobConfig.UpdateTimestamps,
			c.StorageMode, c.AccessLogger, c.ErrorLogger, c.NumUploaders, c.MaxQueuedUploads,
		)
	}

	if c.Peers != nil {
		var remote *http.Client
		if c.Peers.KeyFile != "" && c.Peers.CertFile != "" {
			readCert, err := tls.LoadX509KeyPair(
				c.Peers.CertFile,
				c.Peers.KeyFile,
			)
			if err != nil {
				return err
			}

			config := &tls.Config{
				Certificates: []tls.Certificate{readCert},
			}
			tr := &http.Transport{TLSClientConfig: config}
			remote = &http.Client{Transport: tr}
		} else {
			remote = &http.Client{}
		}

		var updater peerproxy.PeerUpdater
		var err error
		if c.Peers.List != nil {
			updater = peerproxy.NewStaticUpdater(c.Peers.List)
		} else {
			updater, err = peerproxy.NewSRVUpdater(c.Peers.SRV, c.Peers.Interval, c.TLSCertFile != "", c.ErrorLogger)
			if err != nil {
				return err
			}
		}
		proxyCache, err := peerproxy.New(c.Peers.Self, updater, c.ProxyBackend,
			remote, c.AccessLogger, c.ErrorLogger, c.NumUploaders, c.MaxQueuedUploads)

		if err != nil {
			return err
		}

		c.ProxyBackend = proxyCache
	}

	return nil
}

func parseBucketLookupType(typeStr string) (minio.BucketLookupType, error) {
	valMap := map[string]minio.BucketLookupType{
		"auto": minio.BucketLookupAuto,
		"dns":  minio.BucketLookupDNS,
		"path": minio.BucketLookupPath,
	}

	val, ok := valMap[typeStr]
	if !ok {
		return 0, fmt.Errorf("Unsupported value: %s", typeStr)
	}

	return val, nil
}
