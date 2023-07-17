package peerproxy

import (
	"errors"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/golang/groupcache/consistenthash"
)

type peerPicker interface {
	Pick(key string) string
}

func newPeerPicker(self string, updater PeerUpdater, accessLogger, errorLogger cache.Logger) (peerPicker, error) {
	if updater == nil {
		p := &noPicker{}
		return p, nil
	}

	if self == "" {
		return nil, errors.New("self must be provided")
	}

	p := &hashPicker{
		self:  self,
		peers: consistenthash.New(50, nil),
	}

	selfInPeers := func(peers []string) bool {
		for _, peer := range peers {
			if peer == self {
				return true
			}
		}
		return false
	}

	var prev []string
	set := func(peers []string) {
		m := consistenthash.New(50, nil)
		m.Add(peers...)
		p.mu.Lock()
		defer p.mu.Unlock()
		accessLogger.Printf("Updating %d peers: %v", len(peers), peers)
		prev = peers
		p.peers = m
	}

	update := func() {
		peers, err := updater.Update()
		if err != nil {
			errorLogger.Printf("Error while updating peer list: %s", err)
			return
		}

		if !selfInPeers(peers) {
			peers = append(peers, self)
		}

		sort.Strings(peers)
		if !reflect.DeepEqual(prev, peers) {
			set(peers)
		}
	}

	update()

	tick := time.Tick(updater.Interval())
	if tick != nil {
		go func() {
			for range tick {
				update()
			}
		}()
	}
	return p, nil
}

type noPicker struct {
}

func (p *noPicker) Pick(key string) string {
	return ""
}

type hashPicker struct {
	self  string
	peers *consistenthash.Map
	mu    sync.RWMutex
}

func (p *hashPicker) Pick(key string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	peer := p.peers.Get(key)
	if peer != p.self {
		return peer
	}
	return ""
}
