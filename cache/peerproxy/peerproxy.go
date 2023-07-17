// Package peerproxy is a cache implementation that can proxy artifacts
// from/to another bazel-remote instance.
package peerproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type peerProxyCache struct {
	delegate     cache.Proxy
	peerPicker   peerPicker
	remote       *http.Client
	uploadQueue  chan<- backendproxy.UploadReq
	accessLogger cache.Logger
	errorLogger  cache.Logger
}

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_peer_cache_hits",
		Help: "The total number of HTTP backend cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_peer_cache_misses",
		Help: "The total number of HTTP backend cache misses",
	})
)

func New(self string, updater PeerUpdater, delegate cache.Proxy,
	remote *http.Client, accessLogger cache.Logger, errorLogger cache.Logger,
	numUploaders, maxQueuedUploads int) (cache.Proxy, error) {

	peerPicker, err := newPeerPicker(self, updater, accessLogger, errorLogger)
	if err != nil {
		return nil, err
	}

	proxy := &peerProxyCache{
		delegate:     delegate,
		peerPicker:   peerPicker,
		remote:       remote,
		accessLogger: accessLogger,
		errorLogger:  errorLogger,
	}

	proxy.uploadQueue = backendproxy.StartUploaders(proxy, numUploaders, maxQueuedUploads)

	return proxy, nil
}

func (r *peerProxyCache) UploadFile(item backendproxy.UploadReq) {
	if item.LogicalSize == 0 {
		item.Rc.Close()
		// See https://github.com/golang/go/issues/20257#issuecomment-299509391
		item.Rc = http.NoBody
	}

	key := fmt.Sprintf("%s/%s", item.Kind, item.Hash)
	peer := r.peerPicker.Pick(key)
	url := fmt.Sprintf("%s/%s", peer, key)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodHead, url, nil)
	if err != nil {
		r.errorLogger.Printf("INTERNAL ERROR, FAILED TO SETUP PEER PROXY UPLOAD %s: %s", url, err)
		item.Rc.Close()
		return
	}

	rsp, err := r.remote.Do(req)
	if err == nil && rsp.StatusCode == http.StatusOK {
		r.accessLogger.Printf("SKIP UPLOAD %s", item.Hash)
		item.Rc.Close()
		return
	}

	req, err = http.NewRequestWithContext(context.Background(), http.MethodPut, url, item.Rc)
	if err != nil {
		r.errorLogger.Printf("INTERNAL ERROR, FAILED TO SETUP PEER PROXY UPLOAD %s: %s", url, err)

		// item.Rc will be closed if we call req.Do(), but not if we
		// return earlier.
		item.Rc.Close()

		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = item.SizeOnDisk

	rsp, err = r.remote.Do(req)
	if err != nil {
		r.errorLogger.Printf("PEER %s UPLOAD: %s", url, err.Error())
		return
	}
	_, err = io.Copy(io.Discard, rsp.Body)
	if err != nil {
		r.errorLogger.Printf("PEER %s UPLOAD: %s", url, err.Error())
		return
	}
	rsp.Body.Close()

	logResponse(r.accessLogger, "UPLOAD", rsp.StatusCode, url)
}

// Helper function for logging responses
func logResponse(logger cache.Logger, method string, code int, url string) {
	logger.Printf("PEER %s %d %s", method, code, url)
}

func (r *peerProxyCache) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	if r.uploadQueue == nil {
		rc.Close()
		return
	}

	key := fmt.Sprintf("%s/%s", kind, hash)
	peer := r.peerPicker.Pick(key)
	if peer == "" {
		if r.delegate != nil {
			r.delegate.Put(ctx, kind, hash, logicalSize, sizeOnDisk, rc)
		}
		return
	}

	item := backendproxy.UploadReq{
		Hash:        hash,
		LogicalSize: logicalSize,
		SizeOnDisk:  sizeOnDisk,
		Kind:        kind,
		Rc:          rc,
	}

	select {
	case r.uploadQueue <- item:
	default:
		r.errorLogger.Printf("too many uploads queued")
		rc.Close()
	}
}

func (r *peerProxyCache) Get(ctx context.Context, kind cache.EntryKind, hash string) (io.ReadCloser, int64, error) {
	key := fmt.Sprintf("%s/%s", kind, hash)
	peer := r.peerPicker.Pick(key)
	if peer == "" {
		if r.delegate != nil {
			return r.delegate.Get(ctx, kind, hash)
		}
		return nil, -1, nil
	}

	url := fmt.Sprintf("%s/%s", peer, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cacheMisses.Inc()
		return nil, -1, err
	}

	rsp, err := r.remote.Do(req)
	if err != nil {
		cacheMisses.Inc()
		return nil, -1, err
	}

	logResponse(r.accessLogger, "DOWNLOAD", rsp.StatusCode, url)

	if rsp.StatusCode == http.StatusNotFound {
		cacheMisses.Inc()
		return nil, -1, nil
	}

	if rsp.StatusCode != http.StatusOK {
		// If the failed http response contains some data then
		// forward up to 1 KiB.
		var errorBytes []byte
		errorBytes, err = io.ReadAll(io.LimitReader(rsp.Body, 1024))
		var errorText string
		if err == nil {
			errorText = string(errorBytes)
		}

		cacheMisses.Inc()
		return nil, -1, &cache.Error{
			Code: rsp.StatusCode,
			Text: errorText,
		}
	}

	sizeBytesStr := rsp.Header.Get("Content-Length")
	if sizeBytesStr == "" {
		err = errors.New("Missing Content-Length header")
		cacheMisses.Inc()
		return nil, -1, err
	}

	sizeBytesInt, err := strconv.Atoi(sizeBytesStr)
	if err != nil {
		cacheMisses.Inc()
		return nil, -1, err
	}
	sizeBytes := int64(sizeBytesInt)

	cacheHits.Inc()

	return rsp.Body, sizeBytes, nil
}

func (r *peerProxyCache) Contains(ctx context.Context, kind cache.EntryKind, hash string) (bool, int64) {
	key := fmt.Sprintf("%s/%s", kind, hash)
	peer := r.peerPicker.Pick(key)
	if peer == "" {
		if r.delegate != nil {
			return r.delegate.Contains(ctx, kind, hash)
		}
		return false, -1
	}

	url := fmt.Sprintf("%s/%s", peer, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, -1
	}

	rsp, err := r.remote.Do(req)
	if err != nil {
		r.errorLogger.Printf("PEER %s HEAD: %s", url, err)
		return false, -1
	}

	if rsp.StatusCode == http.StatusOK {
		return true, rsp.ContentLength
	}

	return false, -1
}
