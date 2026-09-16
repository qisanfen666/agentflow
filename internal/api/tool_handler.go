// 工具注册表端点。校验与 MCP 转换逻辑全部在 internal/registry，
// handler 只做 HTTP 翻译——与 agent/task handler 同一模式。
package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/qisanfen666/agentflow/internal/registry"
	"github.com/qisanfen666/agentflow/model"
	"github.com/qisanfen666/agentflow/storage"
)

func registerToolRoutes(r *gin.Engine, h *handlers) {
	r.POST("/api/v1/tools", h.registerTool)
	r.GET("/api/v1/tools", h.listTools)
	// /tools/mcp 与 /tools/:id 同段静态+参数并存，gin 按静态优先匹配
	r.GET("/api/v1/tools/mcp", h.listToolsMCP)
	r.GET("/api/v1/tools/:id", h.getTool)
	r.GET("/api/v1/tools/:id/mcp", h.getToolMCP)
}

type toolInput struct {
	Name        string         `json:"name" binding:"required"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
	TimeoutSec  int            `json:"timeout_sec"`
}

func (h *handlers) registerTool(c *gin.Context) {
	var in toolInput
	if err := c.ShouldBindJSON(&in); err != nil {
		badRequest(c, err.Error())
		return
	}
	tool, err := h.deps.Tools.Register(c.Request.Context(), model.ToolDef{
		Name:        in.Name,
		Description: in.Description,
		Parameters:  in.Parameters,
		TimeoutSec:  in.TimeoutSec,
	})
	if err != nil {
		if errors.Is(err, storage.ErrDuplicate) {
			c.JSON(http.StatusConflict, gin.H{"code": "DUPLICATE", "message": err.Error()})
			return
		}
		badRequest(c, err.Error()) // 命名/schema 校验失败都在 400
		return
	}
	c.JSON(http.StatusCreated, tool)
}

func (h *handlers) listTools(c *gin.Context) {
	tools, err := h.deps.Tools.List(c.Request.Context())
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"tools": tools})
}

func (h *handlers) getTool(c *gin.Context) {
	tool, err := h.deps.Tools.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, tool)
}

func (h *handlers) getToolMCP(c *gin.Context) {
	tool, err := h.deps.Tools.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, registry.ToMCP(tool))
}

func (h *handlers) listToolsMCP(c *gin.Context) {
	tools, err := h.deps.Tools.ListMCP(c.Request.Context())
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"tools": tools})
}
