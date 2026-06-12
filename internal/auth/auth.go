package auth

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/api-gateway/internal/config"
	"github.com/golang-jwt/jwt/v5"
)

type Hook interface {
	Apply(r *http.Request) error
}

type InjectHeaderHook struct {
	Header string
	EnvVar string
}

func (h *InjectHeaderHook) Apply(r *http.Request) error {
	val := os.Getenv(h.EnvVar)
	if val == "" {
		return fmt.Errorf("env var %s not set for header %s", h.EnvVar, h.Header)
	}
	r.Header.Set(h.Header, val)
	return nil
}

type Manager struct {
	hooks map[string][]Hook
}

func NewManager() *Manager {
	return &Manager{
		hooks: make(map[string][]Hook),
	}
}

func (mgr *Manager) Register(upstream string, hook Hook) {
	mgr.hooks[upstream] = append(mgr.hooks[upstream], hook)
}

func (mgr *Manager) Apply(upstream string, r *http.Request) error {
	hooks, ok := mgr.hooks[upstream]
	if !ok {
		return nil
	}
	for _, hook := range hooks {
		if err := hook.Apply(r); err != nil {
			return err
		}
	}
	return nil
}

func (mgr *Manager) Clear(upstream string) {
	delete(mgr.hooks, upstream)
}

func (mgr *Manager) Rebuild(upstream string, hookConfigs []config.AuthHookConfig) {
	mgr.Clear(upstream)
	for _, hc := range hookConfigs {
		switch hc.Type {
		case "inject_header":
			mgr.Register(upstream, &InjectHeaderHook{
				Header: hc.Header,
				EnvVar: hc.EnvVar,
			})
		}
	}
}

func (mgr *Manager) RebuildFromConfig(upstream string, hookConfigs []config.AuthHookConfig) {
	mgr.Rebuild(upstream, hookConfigs)
}

func DecodeJWT(tokenStr string) (jwt.MapClaims, error) {
	if strings.HasPrefix(tokenStr, "Bearer ") {
		tokenStr = strings.TrimPrefix(tokenStr, "Bearer ")
	}

	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	token, _, err := parser.ParseUnverified(tokenStr, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("failed to decode JWT: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid JWT claims")
	}

	if exp, ok := claims["exp"]; ok {
		switch v := exp.(type) {
		case float64:
			if time.Now().Unix() > int64(v) {
				return nil, fmt.Errorf("JWT token expired")
			}
		case int64:
			if time.Now().Unix() > v {
				return nil, fmt.Errorf("JWT token expired")
			}
		}
	}

	return claims, nil
}

func ExtractUserID(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}

	claims, err := DecodeJWT(auth)
	if err != nil {
		return ""
	}

	if sub, ok := claims["sub"]; ok {
		if uid, ok := sub.(string); ok {
			return uid
		}
	}
	if uid, ok := claims["user_id"]; ok {
		if s, ok := uid.(string); ok {
			return s
		}
	}
	return ""
}
