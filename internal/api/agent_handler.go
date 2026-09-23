package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/qisanfen666/agentflow/internal/observability"
	"github.com/qisanfen666/agentflow/model"
)

// registerAgentRoutes 注册 Agent 域 6 端点（合同：openapi.yaml /api/v1/agents*）。
func registerAgentRoutes(r gin.IRouter, h *handlers) {
	g := r.Group("/api/v1/agents")
	g.POST("", h.createAgent)
	g.GET("", h.listAgents)
	g.GET("/:id", h.getAgent)
	g.PUT("/:id", h.updateAgent)
	g.DELETE("/:id", h.deleteAgent)
	g.GET("/:id/versions", h.agentVersions)
}

// agentInput 创建请求体（openapi AgentSpecInput）。
type agentInput struct {
	Name            string            `json:"name"`
	Type            string            `json:"type"`
	Runtime         model.RuntimeSpec `json:"runtime"`
	Config          map[string]any    `json:"config"`
	RequireApproval bool              `json:"require_approval"` // 管理员声明高危
}

// agentUpdateInput 更新请求体（AgentSpecUpdate）：嵌入输入 + 乐观锁基准版本。
type agentUpdateInput struct {
	agentInput
	BaseVersion int `json:"base_version"`
}

func (h *handlers) createAgent(c *gin.Context) {
	var in agentInput
	if err := c.ShouldBindJSON(&in); err != nil {
		badRequest(c, err.Error())
		return
	}
	spec := model.AgentSpec{Name: in.Name, Type: in.Type, Runtime: in.Runtime, Config: in.Config,
		RequireApproval: in.RequireApproval}
	// 显式校验先行：把"业务校验失败"和"存储故障"区分成 400 vs 500
	if err := spec.Validate(); err != nil {
		badRequest(c, err.Error())
		return
	}
	created, err := h.deps.Agents.Create(c.Request.Context(), spec)
	if err != nil {
		respondErr(c, err)
		return
	}
	h.audit(c, observability.ActionAgentCreated, observability.EntityAgent, created.ID,
		map[string]any{"runtime_type": created.Runtime.Type})
	c.JSON(http.StatusCreated, created)
}

func (h *handlers) listAgents(c *gin.Context) {
	agents, err := h.deps.Agents.List(c.Request.Context())
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, agents)
}

func (h *handlers) getAgent(c *gin.Context) {
	spec, err := h.deps.Agents.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, spec)
}

func (h *handlers) updateAgent(c *gin.Context) {
	var in agentUpdateInput
	if err := c.ShouldBindJSON(&in); err != nil {
		badRequest(c, err.Error())
		return
	}
	next := model.AgentSpec{Name: in.Name, Type: in.Type, Runtime: in.Runtime, Config: in.Config,
		RequireApproval: in.RequireApproval}
	if err := next.Validate(); err != nil {
		badRequest(c, err.Error())
		return
	}
	updated, err := h.deps.Agents.Update(c.Request.Context(), c.Param("id"), in.BaseVersion, next)
	if err != nil {
		respondErr(c, err) // ErrVersionConflict -> 409
		return
	}
	h.audit(c, observability.ActionAgentUpdated, observability.EntityAgent, updated.ID,
		map[string]any{"from_version": in.BaseVersion, "to_version": updated.Version})
	c.JSON(http.StatusOK, updated)
}

func (h *handlers) deleteAgent(c *gin.Context) {
	if err := h.deps.Agents.SoftDelete(c.Request.Context(), c.Param("id")); err != nil {
		respondErr(c, err)
		return
	}
	h.audit(c, observability.ActionAgentDeleted, observability.EntityAgent, c.Param("id"), nil)
	c.JSON(http.StatusNoContent, nil)
}

func (h *handlers) agentVersions(c *gin.Context) {
	vs, err := h.deps.Agents.Versions(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, vs)
}
