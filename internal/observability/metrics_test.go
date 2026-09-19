package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsNilSafe(t *testing.T) {
	var m *Metrics         // 未启用
	m.IncTask("succeeded") // 不应 panic
	m.IncRetry()
	m.AddTokens(1, 2, "x")
	m.ObserveDuration(time.Second)
}

func TestMetricsRecord(t *testing.T) {
	m := NewMetrics()

	m.IncTask("succeeded")
	m.IncTask("succeeded")
	m.IncTask("failed")
	if got := testutil.ToFloat64(m.Tasks.WithLabelValues("succeeded")); got != 2 {
		t.Fatalf("tasks{succeeded} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.Tasks.WithLabelValues("failed")); got != 1 {
		t.Fatalf("tasks{failed} = %v, want 1", got)
	}

	m.IncRetry()
	m.IncRetry()
	if got := testutil.ToFloat64(m.Retries); got != 2 {
		t.Fatalf("retries = %v, want 2", got)
	}

	m.AddTokens(17, 8, "m1")
	m.AddTokens(10, 5, "") // 空 model 归 unknown
	if got := testutil.ToFloat64(m.Tokens.WithLabelValues("prompt", "m1")); got != 17 {
		t.Fatalf("tokens{prompt,m1} = %v, want 17", got)
	}
	if got := testutil.ToFloat64(m.Tokens.WithLabelValues("completion", "unknown")); got != 5 {
		t.Fatalf("tokens{completion,unknown} = %v, want 5", got)
	}

	m.ObserveDuration(2 * time.Second)
	if n := testutil.CollectAndCount(m.Duration); n != 1 {
		t.Fatalf("duration samples = %d, want 1", n)
	}
}

// 私有 registry：不污染全局（同名指标可再注册一个实例而不冲突）。
func TestMetricsPrivateRegistry(t *testing.T) {
	m1 := NewMetrics()
	m2 := NewMetrics()
	if m1.registry == m2.registry {
		t.Fatal("each Metrics must own a private registry")
	}
	m1.IncTask("failed")
	if got := testutil.ToFloat64(m2.Tasks.WithLabelValues("failed")); got != 0 {
		t.Fatalf("instances must be isolated, got %v", got)
	}
}
