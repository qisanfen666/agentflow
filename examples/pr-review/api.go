// api.go —— 中台业务端点。故事映射：
//
//	阶段1 CI webhook -> webhookPR（也供 GUI 手动建 PR：createPR 走同一提交管线）
//	阶段2 直播       -> streamEvents（SSE 代理：Hub 历史回放 + token 级增量）
//	阶段3 审批       -> approve/reject（admin key 在服务端，浏览器不持凭据）
//	阶段4 回写       -> watcher 终态后拉流聚合，Review 落在 PR 记录上
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type midway struct {
	cfg   envConfig
	cp    *cpClient
	board *board
	mode  string // mock | real（启动时按镜像名判定，GUI 展示用）
}

// boardView GET /api/board 的聚合视图。
type boardView struct {
	Mode         string        `json:"mode"` // mock | real
	Budget       int64         `json:"daily_token_budget"`
	RateLimit    int           `json:"rate_limit_per_min"`
	AuditPath    string        `json:"audit_path"`
	Repos        []repoCat     `json:"repos"`
	Agents       map[string]string `json:"agents"`
	PRs          []pr          `json:"prs"`
	TotalTokens  int64         `json:"total_tokens"`
}

func (m *midway) getBoard(c *gin.Context) {
	view := boardView{
		Budget: m.cfg.dailyBudget, RateLimit: m.cfg.rateLimit,
		AuditPath: m.cfg.auditPath, Repos: m.board.repos,
		PRs: m.board.snapshot(),
	}
	m.board.mu.RLock()
	view.Agents = m.board.agentIDs
	m.board.mu.RUnlock()
	for _, p := range view.PRs {
		if p.Review != nil {
			view.TotalTokens += p.Review.PromptTokens + p.Review.CompletionTokens
		}
	}
	view.Mode = m.mode
	c.JSON(http.StatusOK, view)
}

// prInput 建/webhook PR 的公共入参。
type prInput struct {
	Repo  string `json:"repo" binding:"required"`
	Title string `json:"title"`
	PR    int    `json:"pr"`      // CI 侧已知 PR 号时透传（幂等 key 用它），缺省自增
	Workspace string `json:"workspace"` // CI 显式提供 PR 检出目录（自托管 runner 与中台同机）
	Diff      string `json:"diff"`      // CI 显式提供补丁文本（git diff 产物）
}

// createPR GUI 手动建 PR（模拟 CI 触发同一条管线）。
func (m *midway) createPR(c *gin.Context) {
	var in prInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	p, err := m.submitPR(c.Request.Context(), in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, p)
}

// webhookPR CI 入口（预留：cicd/github-actions-pr-review.yml.example 调这里）。
// 响应 202 与任务 ID；幂等由控制面 Idempotency-Key 保证（webhook 重发安全）。
func (m *midway) webhookPR(c *gin.Context) {
	var in prInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	p, err := m.submitPR(c.Request.Context(), in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"pr": p.Number, "repo": p.Repo, "task_id": p.TaskID, "task_status": p.TaskStatus,
		"session_id": sessionID(p), "agent_id": p.AgentID,
	})
}

// submitReview 对既有 PR 重新提交评审（前次已终态时允许；排演返工场景）。
func (m *midway) submitReview(c *gin.Context) {
	num := c.Param("num")
	var n int
	if _, err := fmt.Sscanf(num, "%d", &n); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad pr number"})
		return
	}
	p, ok := m.board.get(n)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "pr not found"})
		return
	}
	if p.TaskStatus != "" && !isFinal(p.TaskStatus) {
		c.JSON(http.StatusConflict, gin.H{"error": "task still in flight: " + p.TaskStatus})
		return
	}
	in := prInput{Repo: p.Repo, Title: p.Title, PR: p.Number}
	np, err := m.submitPR(c.Request.Context(), in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, np)
}

// approve/reject 审批操作（阶段 3）。凭据（admin key）只在中台进程内。
func (m *midway) approve(c *gin.Context) { m.reviewAction(c, "approve") }
func (m *midway) reject(c *gin.Context)  { m.reviewAction(c, "reject") }

func (m *midway) reviewAction(c *gin.Context, action string) {
	p, ok := m.lookup(c)
	if !ok {
		return
	}
	if p.TaskStatus != "pending_approval" {
		c.JSON(http.StatusConflict, gin.H{"error": "task not awaiting review: " + p.TaskStatus})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	var out struct {
		Status string `json:"status"`
	}
	if _, err := m.cp.do(ctx, "POST", "/api/v1/tasks/"+p.TaskID+"/"+action, m.cp.adminKey, nil, &out, nil); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	m.board.update(p.Number, func(x *pr) { x.TaskStatus = out.Status })
	c.JSON(http.StatusAccepted, gin.H{"status": out.Status})
}

func (m *midway) lookup(c *gin.Context) (*pr, bool) {
	var n int
	if _, err := fmt.Sscanf(c.Param("num"), "%d", &n); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad pr number"})
		return nil, false
	}
	p, ok := m.board.get(n)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "pr not found"})
		return nil, false
	}
	return p, true
}

func sessionID(p *pr) string { return fmt.Sprintf("pr-%s-%d", p.Repo, p.Number) }

// metricsProxy /metrics（admin-only）代取：浏览器不持 key，凭据留在中台进程内。
func (m *midway) metricsProxy(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cp.base+"/metrics", nil)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	req.Header.Set("X-API-Key", m.cp.adminKey)
	resp, err := m.cp.hc.Do(req)
	if err != nil {
		c.String(http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()
	c.Data(resp.StatusCode, "text/plain; version=0.0.4", func() []byte {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		return b
	}())
}
