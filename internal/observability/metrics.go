// 指标定义与记录。M5：控制面自己知道的量才有指标——状态流转、退避重投、
// usage 归集；业务语义量（队列深度等）需要跨组件加接口，v0.1 API 冻结期不做。
package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics 指标集。使用私有 registry 而非全局注册：库不应改动宿主进程级状态——
// 嵌入模式下宿主可能有自己的 Prometheus 体系，全局注册会混入宿主指标。
// 指标字段导出供测试读取；埋点走 nil-safe 方法。
type Metrics struct {
	registry *prometheus.Registry

	// Tasks 任务终态计数（status: succeeded | failed | timeout | cancelled）
	Tasks *prometheus.CounterVec
	// Retries 可重试错误的退避重投次数（不含首跑）
	Retries prometheus.Counter
	// Tokens usage 事件归集的 token 累计（direction: prompt | completion; model）
	Tokens *prometheus.CounterVec
	// Duration 单次执行耗时（task_started 到终态；重试各段独立观测）
	Duration prometheus.Histogram
}

// NewMetrics 构建指标集并注册进私有 registry。
func NewMetrics() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		Tasks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "agentflow_tasks_total",
			Help: "Tasks by final status",
		}, []string{"status"}),
		Retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "agentflow_task_retries_total",
			Help: "Backoff re-queues after retryable errors",
		}),
		Tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "agentflow_tokens_total",
			Help: "Token usage aggregated from usage events",
		}, []string{"direction", "model"}),
		Duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "agentflow_task_duration_seconds",
			Help:    "Wall time of one execution attempt (started to final)",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s ~ ~7min
		}),
	}
	m.registry.MustRegister(m.Tasks, m.Retries, m.Tokens, m.Duration)
	return m
}

// Handler 暴露 Prometheus 抓取端点（GET /metrics）。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// 以下记录方法全部 nil-safe：Metrics 未启用（nil）时埋点为空操作，
// 调用方无需判空。

// IncTask 终态计数。
func (m *Metrics) IncTask(status string) {
	if m == nil {
		return
	}
	m.Tasks.WithLabelValues(status).Inc()
}

// IncRetry 退避重投计数。
func (m *Metrics) IncRetry() {
	if m == nil {
		return
	}
	m.Retries.Inc()
}

// AddTokens 累计一次 usage 事件的 token 数。model 为空归入 "unknown"。
func (m *Metrics) AddTokens(prompt, completion int64, model string) {
	if m == nil {
		return
	}
	if model == "" {
		model = "unknown"
	}
	m.Tokens.WithLabelValues("prompt", model).Add(float64(prompt))
	m.Tokens.WithLabelValues("completion", model).Add(float64(completion))
}

// ObserveDuration 观测单次执行耗时。
func (m *Metrics) ObserveDuration(d time.Duration) {
	if m == nil {
		return
	}
	m.Duration.Observe(d.Seconds())
}
