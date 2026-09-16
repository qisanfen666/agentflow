// Package api 提供 HTTP 接口层。internal 包：实现细节，不对外承诺稳定性。
// 路由合同见 docs/api/openapi.yaml。
package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/qisanfen666/agentflow/internal/dispatch"
	"github.com/qisanfen666/agentflow/internal/engine"
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
	Queue      engine.Queue       // M2：提交即入队，由 engine.Worker 消费执行
	Idem       storage.IdemStore  // 提交幂等占位（memory / redis 双实现）
	Tools      *registry.Registry // M3：工具注册表（校验 + MCP 导出）
}

// handlers 共享依赖的 handler 集合。各端点方法分属 agent_handler.go / task_handler.go。
type handlers struct {
	deps Dependencies
}

// NewRouter 构建 gin 引擎。gin.SetMode 由调用方决定（测试用 TestMode）。
func NewRouter(deps Dependencies) *gin.Engine {
	r := gin.New()
	// 只挂 Recovery：日志/认证/CORS 属横切关注点，由 facade 按配置挂载
	r.Use(gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	h := &handlers{deps: deps}
	registerAgentRoutes(r, h) // 定义于 agent_handler.go
	registerTaskRoutes(r, h)  // 定义于 task_handler.go
	registerToolRoutes(r, h)  // 定义于 tool_handler.go
	return r
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
