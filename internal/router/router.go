package router

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/api-gateway/internal/config"
)

type Match struct {
	Upstream    string
	WebSocket   bool
	StripPrefix string
	AddPrefix   string
	PathRewrite string
	Route       *config.RouteConfig
}

type entry struct {
	prefix string
	route  *config.RouteConfig
}

type snapshot struct {
	entries []entry
}

type Router struct {
	current atomic.Pointer[snapshot]
	mu      sync.RWMutex
}

func New() *Router {
	r := &Router{}
	r.current.Store(&snapshot{})
	return r
}

func (r *Router) Load(routes []config.RouteConfig) {
	entries := make([]entry, 0, len(routes))
	for i := range routes {
		entries = append(entries, entry{
			prefix: routes[i].PathPrefix,
			route:  &routes[i],
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return len(entries[i].prefix) > len(entries[j].prefix)
	})

	r.current.Store(&snapshot{entries: entries})
}

func (r *Router) Match(path string) *Match {
	s := r.current.Load()
	if s == nil {
		return nil
	}

	var best *entry
	for i := range s.entries {
		if strings.HasPrefix(path, s.entries[i].prefix) {
			best = &s.entries[i]
			break
		}
	}

	if best == nil {
		return nil
	}

	rewritten := rewritePath(path, best.route.StripPrefix, best.route.AddPrefix)

	return &Match{
		Upstream:    best.route.Upstream,
		WebSocket:   best.route.WebSocket,
		StripPrefix: best.route.StripPrefix,
		AddPrefix:   best.route.AddPrefix,
		PathRewrite: rewritten,
		Route:       best.route,
	}
}

func rewritePath(original, stripPrefix, addPrefix string) string {
	if stripPrefix == "" && addPrefix == "" {
		return original
	}

	result := original

	if stripPrefix != "" {
		result = strings.TrimPrefix(result, stripPrefix)
	}

	if addPrefix != "" {
		result = addPrefix + result
	}

	if !strings.HasPrefix(result, "/") {
		result = "/" + result
	}

	return cleanPath(result)
}

func cleanPath(p string) string {
	if p == "" {
		return "/"
	}

	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}

	parts := strings.Split(p, "/")
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if part == ".." {
			if len(cleaned) > 0 {
				cleaned = cleaned[:len(cleaned)-1]
			}
			continue
		}
		cleaned = append(cleaned, part)
	}

	result := "/" + strings.Join(cleaned, "/")
	if strings.HasSuffix(p, "/") && len(cleaned) > 0 {
		result += "/"
	}
	return result
}

func (r *Router) Routes() []config.RouteConfig {
	s := r.current.Load()
	if s == nil {
		return nil
	}
	routes := make([]config.RouteConfig, 0, len(s.entries))
	for _, e := range s.entries {
		routes = append(routes, *e.route)
	}
	return routes
}
