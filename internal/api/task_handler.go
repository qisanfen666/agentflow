package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/qisanfen666/agentflow/model"
)

// registerTaskRoutes 注册 Task 域 4 端点（合同：openapi.yaml /api/v1/tasks*）。
func registerTaskRoutes(r *gin.Engine, h *handlers) {
	g := r.Group("/api/v1/tasks")
	g.POST("", h.submitTask)
	g.GET("/:id", h.getTask)
	g.DELETE("/:id", h.cancelTask)
	g.GET("/:id/stream", h.streamTask)
}

// idemTTL 幂等 key 的占位窗口：窗口内同 key 视为重复提交。
const idemTTL = 10 * time.Minute

// taskInput 提交请求体（openapi TaskInput）。
type taskInput struct {
	AgentID      string         `json:"agent_id"`
	AgentVersion int            `json:"agent_version"` // 0 = 锁定当前最新版本
	Payload      map[string]any `json:"payload"`
	TimeoutSec   int            `json:"timeout_sec"`
}

// submitTask 提交任务：校验 Agent -> 锁定版本 -> 创建 pending -> 直接触发分发（M1 无队列）。
func (h *handlers) submitTask(c *gin.Context) {
	var in taskInput
	if err := c.ShouldBindJSON(&in); err != nil {
		badRequest(c, err.Error())
		return
	}
	if in.AgentID == "" {
		badRequest(c, "agent_id is required")
		return
	}

	// 版本锁定：解析出确切的 (agent_id, version) 并固化进任务
	var spec model.AgentSpec
	var err error
	if in.AgentVersion == 0 {
		spec, err = h.deps.Agents.Get(c.Request.Context(), in.AgentID)
	} else {
		spec, err = h.deps.Agents.GetVersion(c.Request.Context(), in.AgentID, in.AgentVersion)
	}
	if err != nil {
		respondErr(c, err) // 404
		return
	}
	if spec.Deleted() {
		c.JSON(http.StatusNotFound, gin.H{"code": codeNotFound, "message": "agent is deleted"})
		return
	}

	task, err := h.deps.Tasks.Create(c.Request.Context(), model.Task{
		Status:       model.TaskPending,
		AgentID:      spec.ID,
		AgentVersion: spec.Version, // 锁定：后续 Agent 更新不影响本任务
		Payload:      in.Payload,
		TimeoutSec:   in.TimeoutSec,
	})
	if err != nil {
		respondErr(c, err)
		return
	}

	// 幂等（task-state.md 第 4 节）：原子占位，同 key 的重复提交返回原任务。
	// 输家手里刚创建的任务成了孤儿（pending 且永不入队）——迁移 cancelled 收尾，
	// 不留永远 pending 的僵尸。
	if key := c.GetHeader("Idempotency-Key"); key != "" {
		existing, inserted, err := h.deps.Idem.PutIfAbsent(c.Request.Context(), key, task.ID, idemTTL)
		if err != nil {
			respondErr(c, err)
			return
		}
		if !inserted {
			_ = task.Transition(model.TaskCancelled)
			_ = h.deps.Tasks.Save(c.Request.Context(), task)
			if orig, gerr := h.deps.Tasks.Get(c.Request.Context(), existing); gerr == nil {
				c.JSON(http.StatusAccepted, orig)
				return
			}
			c.JSON(http.StatusAccepted, task) // 原任务读不到（极端情况）：退回自身
			return
		}
	}

	// M2：提交即入队；执行由 engine.Worker 异步消费（队列化代价：
	// 提交后短暂 pending，毫秒级）
	if err := h.deps.Queue.Enqueue(c.Request.Context(), task); err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, task)
}

func (h *handlers) getTask(c *gin.Context) {
	task, err := h.deps.Tasks.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, task)
}

// cancelTask 显式取消：迁移状态 -> 持久化 -> 通知分发器取消 ctx（尽力传播）。
func (h *handlers) cancelTask(c *gin.Context) {
	task, err := h.deps.Tasks.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	if task.Status.Final() {
		// 终态不可迁移：不是错误重试，而是明确告知冲突
		c.JSON(http.StatusConflict, gin.H{"code": codeConflict, "message": "task already in final state " + string(task.Status)})
		return
	}
	if err := task.Transition(model.TaskCancelled); err != nil {
		respondErr(c, err)
		return
	}
	if err := h.deps.Tasks.Save(c.Request.Context(), task); err != nil {
		respondErr(c, err)
		return
	}
	h.deps.Dispatcher.Cancel(task.ID)
	c.JSON(http.StatusAccepted, task)
}

// streamTask SSE 透传：把 Hub 中该任务的事件流（含历史回放）按协议原样发给客户端。
// 订阅循环模式：Since 游标 -> 写事件 -> 未结束则等 Notify 唤醒 -> 客户端断开则退出。
func (h *handlers) streamTask(c *gin.Context) {
	taskID := c.Param("id")
	if _, err := h.deps.Tasks.Get(c.Request.Context(), taskID); err != nil {
		respondErr(c, err)
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	// 告知反向代理禁用缓冲，保证 token 级实时性
	c.Header("X-Accel-Buffering", "no")
	w := c.Writer

	notify := h.deps.Hub.Notify(taskID) // 先订阅再取快照，避免错过事件
	ctx := c.Request.Context()
	i := 0
	for {
		evs, finished, ok := h.deps.Hub.Since(taskID, i)
		if !ok {
			// 任务存在但尚未发布任何事件（刚提交的瞬间），等唤醒
			select {
			case <-notify:
				continue
			case <-ctx.Done():
				return
			}
		}
		for _, ev := range evs {
			data, err := json.Marshal(ev)
			if err != nil {
				continue // 不可序列化事件直接跳过，不打断流
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
		}
		w.Flush()
		i += len(evs)

		if finished {
			fmt.Fprint(w, "data: [DONE]\n\n")
			w.Flush()
			return
		}

		select {
		case <-notify:
		case <-ctx.Done():
			return // 客户端断开，停止透传（任务继续执行不受影响）
		}
	}
}
