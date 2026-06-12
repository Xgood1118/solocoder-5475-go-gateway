package ipblacklist

import (
	"net"
	"net/http"
	"sync"
)

type IPBlacklist struct {
	networks []*net.IPNet
	mu       sync.RWMutex
}

func New() *IPBlacklist {
	return &IPBlacklist{
		networks: make([]*net.IPNet, 0),
	}
}

func (bl *IPBlacklist) Load(cidrs []string) error {
	networks := make([]*net.IPNet, 0, len(cidrs))
	for _, cidr := range cidrs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			return err
		}
		networks = append(networks, network)
	}

	bl.mu.Lock()
	bl.networks = networks
	bl.mu.Unlock()
	return nil
}

func (bl *IPBlacklist) IsBlocked(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	bl.mu.RLock()
	defer bl.mu.RUnlock()

	for _, network := range bl.networks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func (bl *IPBlacklist) CheckRequest(r *http.Request) bool {
	ip := extractIP(r)
	return bl.IsBlocked(ip)
}

func extractIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return splitFirst(xff)
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func splitFirst(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			return s[:i]
		}
	}
	return s
}
