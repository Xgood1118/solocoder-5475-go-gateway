package openapi

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/api-gateway/internal/config"
)

type Document struct {
	OpenAPI    string                       `json:"openapi"`
	Info       Info                         `json:"info"`
	Paths      map[string]map[string]*Path  `json:"paths"`
	Components Components                   `json:"components,omitempty"`
}

type Info struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

type Path struct {
	Summary     string            `json:"summary,omitempty"`
	Description string            `json:"description,omitempty"`
	OperationID string            `json:"operationId,omitempty"`
	Tags        []string          `json:"tags"`
	Parameters  []Parameter       `json:"parameters,omitempty"`
	Responses   map[string]Resp   `json:"responses"`
	Security    []map[string][]string `json:"security,omitempty"`
	XUpstream   string            `json:"x-upstream,omitempty"`
	XRateLimit  *RateLimitInfo    `json:"x-rate-limit,omitempty"`
	XAuth       *AuthInfo         `json:"x-auth,omitempty"`
}

type Parameter struct {
	Name        string `json:"name"`
	In          string `json:"in"`
	Required    bool   `json:"required"`
	Description string `json:"description,omitempty"`
	Schema      Schema `json:"schema"`
}

type Schema struct {
	Type string `json:"type"`
}

type Resp struct {
	Description string `json:"description"`
}

type RateLimitInfo struct {
	RPS           int    `json:"rps,omitempty"`
	MaxConcurrent int    `json:"max_concurrent,omitempty"`
	Mode          string `json:"mode,omitempty"`
}

type AuthInfo struct {
	Type string `json:"type,omitempty"`
}

type Components struct {
	SecuritySchemes map[string]SecurityScheme `json:"securitySchemes,omitempty"`
}

type SecurityScheme struct {
	Type        string `json:"type"`
	Scheme      string `json:"scheme,omitempty"`
	In          string `json:"in,omitempty"`
	Name        string `json:"name,omitempty"`
}

type Generator struct {
	cfg config.OpenAPIConfig
}

func NewGenerator(cfg config.OpenAPIConfig) *Generator {
	return &Generator{cfg: cfg}
}

func (g *Generator) Generate(routes []config.RouteConfig) map[string]*Document {
	byUpstream := make(map[string][]config.RouteConfig)
	for _, route := range routes {
		byUpstream[route.Upstream] = append(byUpstream[route.Upstream], route)
	}

	docs := make(map[string]*Document)
	for upstream, upstreamRoutes := range byUpstream {
		docs[upstream] = g.generateForUpstream(upstream, upstreamRoutes)
	}
	return docs
}

func (g *Generator) generateForUpstream(upstream string, routes []config.RouteConfig) *Document {
	doc := &Document{
		OpenAPI: "3.0.3",
		Info: Info{
			Title:       g.cfg.Title + " - " + upstream,
			Version:     g.cfg.Version,
			Description: g.cfg.Description,
		},
		Paths: make(map[string]map[string]*Path),
		Components: Components{
			SecuritySchemes: map[string]SecurityScheme{
				"bearerAuth": {
					Type:   "http",
					Scheme: "bearer",
				},
			},
		},
	}

	sort.Slice(routes, func(i, j int) bool {
		return routes[i].PathPrefix < routes[j].PathPrefix
	})

	for _, route := range routes {
		path := route.PathPrefix
		if strings.HasSuffix(path, "/*") {
			path = strings.TrimSuffix(path, "*")
		}
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}

		if doc.Paths[path] == nil {
			doc.Paths[path] = make(map[string]*Path)
		}

		methods := []string{"get", "post", "put", "delete", "patch"}
		for _, method := range methods {
			op := &Path{
				Summary:     method + " " + path,
				OperationID: method + "_" + strings.ReplaceAll(strings.Trim(path, "/"), "/", "_"),
				Tags:        []string{upstream},
				Responses: map[string]Resp{
					"200": {Description: "Success"},
					"400": {Description: "Bad Request"},
					"401": {Description: "Unauthorized"},
					"429": {Description: "Rate Limited"},
					"500": {Description: "Internal Server Error"},
					"503": {Description: "Service Unavailable"},
				},
				XUpstream: upstream,
			}

			if route.RateLimit.RPS > 0 || route.RateLimit.MaxConcurrent > 0 {
				op.XRateLimit = &RateLimitInfo{
					RPS:           route.RateLimit.RPS,
					MaxConcurrent: route.RateLimit.MaxConcurrent,
					Mode:          route.RateLimit.Mode,
				}
			}

			if len(route.AuthHooks) > 0 {
				op.XAuth = &AuthInfo{Type: "custom"}
				op.Security = []map[string][]string{{"bearerAuth": {}}}
			}

			doc.Paths[path][method] = op
		}
	}

	return doc
}

func WriteDocs(docs map[string]*Document, w http.ResponseWriter, upstream string) error {
	w.Header().Set("Content-Type", "application/json")

	if upstream != "" {
		doc, ok := docs[upstream]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "upstream not found"})
			return nil
		}
		return json.NewEncoder(w).Encode(doc)
	}

	all := make(map[string]*Document)
	for k, v := range docs {
		all[k] = v
	}
	return json.NewEncoder(w).Encode(all)
}

func UpstreamList(docs map[string]*Document) []string {
	list := make([]string, 0, len(docs))
	for k := range docs {
		list = append(list, k)
	}
	sort.Strings(list)
	return list
}
