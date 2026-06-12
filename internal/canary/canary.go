package canary

import (
	"hash/fnv"
	"net/http"

	"github.com/api-gateway/internal/config"
)

type Decision struct {
	Upstream  string
	IsCanary  bool
	Variant   string
}

func Decide(r *http.Request, canaryCfg config.CanaryConfig, stainCfg config.StainConfig) Decision {
	if !canaryCfg.Enabled || len(canaryCfg.Variants) == 0 {
		return Decision{
			Upstream: canaryCfg.Variants[0].Upstream,
			IsCanary: false,
		}
	}

	if len(canaryCfg.Variants) == 1 {
		return Decision{
			Upstream: canaryCfg.Variants[0].Upstream,
			IsCanary: false,
		}
	}

	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = r.RemoteAddr
	}

	h := fnv.New32a()
	h.Write([]byte(requestID))
	hashVal := h.Sum32()

	totalWeight := 0
	for _, v := range canaryCfg.Variants {
		totalWeight += v.Weight
	}
	if totalWeight == 0 {
		return Decision{
			Upstream: canaryCfg.Variants[0].Upstream,
			IsCanary: false,
		}
	}

	point := hashVal % uint32(totalWeight)
	cumulative := 0
	selected := canaryCfg.Variants[0]
	isCanary := false

	for i, v := range canaryCfg.Variants {
		cumulative += v.Weight
		if point < uint32(cumulative) {
			selected = v
			isCanary = i > 0
			break
		}
	}

	if isCanary {
		applyStain(r, stainCfg)
	}

	return Decision{
		Upstream: selected.Upstream,
		IsCanary: isCanary,
		Variant:  selected.Upstream,
	}
}

func applyStain(r *http.Request, stainCfg config.StainConfig) {
	if stainCfg.CanaryHeader != "" {
		if r.Header.Get(stainCfg.CanaryHeader) == "" {
			r.Header.Set(stainCfg.CanaryHeader, "true")
		}
	}
	for k, v := range stainCfg.CustomHeaders {
		if r.Header.Get(k) == "" {
			r.Header.Set(k, v)
		}
	}
}

func IsCanaryRequest(r *http.Request, canaryHeader string) bool {
	if canaryHeader == "" {
		return false
	}
	return r.Header.Get(canaryHeader) == "true"
}
