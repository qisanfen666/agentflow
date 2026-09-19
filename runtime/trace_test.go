package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestTraceparentInjection 出站请求必须携带 W3C traceparent，且其 trace id
// 与本地记录的 client span 一致——执行面 Agent 据此把自身 span 挂进同一条链路。
func TestTraceparentInjection(t *testing.T) {
	// 临时接管全局 provider，测试结束恢复
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	var mu sync.Mutex
	var traceparent string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		traceparent = r.Header.Get("traceparent")
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		sse(w,
			`{"type":"token","content":"hi"}`,
			`{"type":"done","task_id":"t_test"}`,
			"[DONE]",
		)
	}))
	defer ts.Close()

	rt := NewPythonHTTP()
	evs := collect(rt.Execute(context.Background(), testReq(ts.URL)))
	if len(evs) == 0 || evs[len(evs)-1].Type != EventDone {
		t.Fatalf("unexpected events: %+v", evs)
	}

	mu.Lock()
	defer mu.Unlock()
	if traceparent == "" {
		t.Fatal("outbound request must carry traceparent header")
	}
	// traceparent 格式：version-traceid-spanid-flags；traceid 是第 2 段
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || len(parts[1]) != 32 {
		t.Fatalf("malformed traceparent: %q", traceparent)
	}

	var found bool
	for _, span := range rec.Ended() {
		if span.Name() == "agent.http POST /run/stream" {
			found = true
			if span.SpanContext().TraceID().String() != parts[1] {
				t.Fatalf("header trace id %s != span trace id %s",
					parts[1], span.SpanContext().TraceID())
			}
		}
	}
	if !found {
		t.Fatal("client span not recorded")
	}
}
