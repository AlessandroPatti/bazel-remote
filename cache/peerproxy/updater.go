package peerproxy

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
)

type PeerUpdater interface {
	Update() ([]string, error)
	Interval() time.Duration
}

type staticUpdater struct {
	peers []string
}

func NewStaticUpdater(peers []string) PeerUpdater {
	return &staticUpdater{
		peers: peers,
	}
}

func (u *staticUpdater) Update() ([]string, error) {
	return u.peers, nil
}

func (u *staticUpdater) Interval() time.Duration {
	return 0 * time.Second
}

type srvUpdater struct {
	srv         string
	interval    time.Duration
	schema      string
	errorLogger cache.Logger
}

func NewSRVUpdater(srv string, interval time.Duration, tls bool, errorLogger cache.Logger) (PeerUpdater, error) {
	if srv == "" {
		return nil, errors.New("SRV cannot be empty")
	}

	schema := "http"
	if tls {
		schema = "https"
	}
	return &srvUpdater{
		srv:         srv,
		interval:    interval,
		schema:      schema,
		errorLogger: errorLogger,
	}, nil
}

func (u *srvUpdater) Update() ([]string, error) {
	_, srvs, err := net.LookupSRV("", "", u.srv)
	if err != nil {
		// LookupSRV can return partial results when one of the
		// records in the response is invalid, but not all of them are.
		// In that case, use the remaining records and log the error
		// otherwise just return the error
		if srvs == nil {
			return nil, err
		}
		u.errorLogger.Printf("Error during SRV lookup: %s", err)
	}

	peers := make([]string, len(srvs))
	for i, s := range srvs {
		ip, err := net.LookupHost(s.Target)
		if err != nil {
			u.errorLogger.Printf("Error during Host lookup: %s", err)
		} else {
			if len(ip) == 0 {
				u.errorLogger.Printf("No IPs found for %s.", s.Target)
			} else {
				peers[i] = fmt.Sprintf("%s://%s:%d", u.schema, ip[0], s.Port)
			}
		}
	}
	return peers, nil
}

func (u *srvUpdater) Interval() time.Duration {
	return u.interval
}
