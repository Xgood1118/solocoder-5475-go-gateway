package tracing

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
)

const (
	traceparentHeader = "traceparent"
	tracestateHeader  = "tracestate"
	versionByte       = "00"
)

var spanCounter atomic.Uint64

func init() {
	var seed [8]byte
	rand.Read(seed[:])
	spanCounter.Store(binary.BigEndian.Uint64(seed[:]))
}

type SpanContext struct {
	TraceID    string
	ParentID   string
	SpanID     string
	TraceFlags string
	TraceState string
}

func nextSpanID() string {
	id := spanCounter.Add(1)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)
	return hex.EncodeToString(b)[:16]
}

func GenerateTraceID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func ExtractOrCreate(r *http.Request) *SpanContext {
	tp := r.Header.Get(traceparentHeader)
	if tp != "" {
		if sc, err := parseTraceparent(tp); err == nil {
			parentID := sc.SpanID
			sc.ParentID = parentID
			sc.SpanID = nextSpanID()
			sc.TraceState = r.Header.Get(tracestateHeader)
			return sc
		}
	}

	return &SpanContext{
		TraceID:    GenerateTraceID(),
		SpanID:     nextSpanID(),
		TraceFlags: "01",
	}
}

func parseTraceparent(tp string) (*SpanContext, error) {
	parts := strings.Split(tp, "-")
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid traceparent format")
	}

	if parts[0] != versionByte {
		return nil, fmt.Errorf("unsupported traceparent version: %s", parts[0])
	}

	traceID := parts[1]
	spanID := parts[2]
	flags := parts[3]

	if len(traceID) != 32 || len(spanID) != 16 {
		return nil, fmt.Errorf("invalid traceparent id lengths")
	}

	return &SpanContext{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: flags,
	}, nil
}

func (sc *SpanContext) Inject(r *http.Request) {
	tp := fmt.Sprintf("%s-%s-%s-%s", versionByte, sc.TraceID, sc.SpanID, sc.TraceFlags)
	r.Header.Set(traceparentHeader, tp)
	if sc.TraceState != "" {
		r.Header.Set(tracestateHeader, sc.TraceState)
	}
}

func (sc *SpanContext) Traceparent() string {
	return fmt.Sprintf("%s-%s-%s-%s", versionByte, sc.TraceID, sc.SpanID, sc.TraceFlags)
}

func (sc *SpanContext) ParentSpanID() string {
	return sc.ParentID
}
