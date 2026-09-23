// Package api 提供 HTTP 接口层。internal 包：实现细节，不对外承诺稳定性。
// 路由合同见 docs/api/openapi.yaml。
package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/qisanfen666/agentflow/internal/dispatch"
	"github.com/qisanfen666/agentflow/internal/engine"
	"github.com/qisanfen666/agentflow/internal/observability"
	"github.com/qisanfen666/agentflow/internal/policy"
	"github.com/qisanfen666/agentflow/internal/registry"
	"github.com/qisanfen666/agentflow/model"
	"github.com/qisanfen666/agentflow/storage"
)

// Dependencies 由调用方（main / 测试 / facade）手工注入的组件。
type Dependencies struct {
	Agents     storage.AgentStore
	Tasks      storage.TaskStore
	Dispatcher *dispatch.Dispatcher
	Hub        *dispatch.Hub
	Queue      engine.Queue              // 提交即入队，由 engine.Worker 消费执行
	Idem       storage.IdemStore         // 提交幂等占位（memory / redis 双实现）
	Tools      *registry.Registry        // 工具注册表（校验 + MCP 导出）
	Audit      observability.AuditLogger // API 层审计（创建/更新/删除/提交/取消）；nil = 关闭
	Metrics    *observability.Metrics    // 非 nil 时挂 GET /metrics
	Auth       gin.HandlerFunc           // 认证授权中间件；nil = 关闭。可注入宿主自定义实现
	Policy     policy.Chain              // 提交前治理规则链；空链 = 不治理
}

// handlers 共享依赖的 handler 集合。各端点方法分属 agent_handler.go / task_handler.go。
type handlers struct {
	deps Dependencies
}

// NewRouter 构建独立 gin 引擎（自带 Recovery）。gin.SetMode 由调用方决定。
func NewRouter(deps Dependencies) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	MountRoutes(r, deps)
	return r
}

// trace 根 span 中间件：提取入站 traceparent -> 开 SERVER span -> 请求 ctx 携带。
// 未启用 tracing 时（全局 Noop provider）span.Start 近似空操作。
// 观测中间件是"不挂业务中间件"约定的显式例外：链路提取必须在最外层。
var apiTracer = otel.Tracer("agentflow/api")

func traceMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := otel.GetTextMapPropagator().Extract(
			c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
		ctx, span := apiTracer.Start(ctx, "http "+c.Request.Method+" "+c.FullPath(),
			trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		if span.IsRecording() {
			span.SetAttributes(attribute.Int("http.response.status_code", c.Writer.Status()))
		}
	}
}

// MountRoutes 把全部端点注册到既有 engine / router group（嵌入模式）。
// 业务中间件（日志/CORS）由宿主自定；两个例外：
//   - trace：观测链路提取必须在最外层
//   - deps.Auth 非 nil 时挂认证授权（内置实现或宿主注入）
//
// /health 先于 auth 注册以保持豁免（探活不带凭据）；/metrics 受 admin 保护。
// deps.Metrics 非 nil 时额外挂 GET /metrics（Prometheus 抓取端点）。
func MountRoutes(r gin.IRouter, deps Dependencies) {
	r.Use(traceMiddleware())
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	if deps.Auth != nil {
		r.Use(deps.Auth)
	}
	if deps.Metrics != nil {
		r.GET("/metrics", gin.WrapH(deps.Metrics.Handler()))
	}

	h := &handlers{deps: deps}
	registerAgentRoutes(r, h) // 定义于 agent_handler.go
	registerTaskRoutes(r, h)  // 定义于 task_handler.go
	registerToolRoutes(r, h)  // 定义于 tool_handler.go
}

// ---------- 错误响应统一映射 ----------

// HTTP 层呈现码。与 model 错误分类学（Agent 执行域）互补，仅用于传输层。
const (
	codeNotFound   = "NOT_FOUND"
	codeConflict   = "VERSION_CONFLICT"
	codeBadRequest = "INVALID_PAYLOAD"
)

// respondErr 把领域错误映射为 HTTP 状态码 + code/message（合同 Error schema）。
// 映射表：storage.ErrNotFound -> 404；ErrVersionConflict -> 409；其余 -> 500。
func respondErr(c *gin.Context, err error) {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"code": codeNotFound, "message": err.Error()})
	case errors.Is(err, storage.ErrVersionConflict):
		c.JSON(http.StatusConflict, gin.H{"code": codeConflict, "message": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"code": model.ErrInternal, "message": err.Error()})
	}
}

// badRequest 统一 400 响应（参数绑定/校验失败）。
func badRequest(c *gin.Context, msg string) {
	c.JSON(http.StatusBadRequest, gin.H{"code": codeBadRequest, "message": msg})
}

// audit 尽力审计 API 层动作（nil 安全，写失败不阻断请求）。
func (h *handlers) audit(c *gin.Context, action, entity, id string, detail map[string]any) {
	observability.RecordBestEffort(h.deps.Audit, c.Request.Context(), observability.AuditEvent{
		Action: action, Entity: entity, EntityID: id, Detail: detail,
	})
}
