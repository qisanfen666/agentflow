package main

// api.go —— 中台业务端点。故事映射：
//
//	阶段1 CI 触发 -> webhookPR（固定 submitter 身份）；GUI createPR 走当前视角身份
//	阶段2 直播    -> streamEvents（SSE 代理：Hub 历史回放 + token 级增量）
//	阶段3 审批    -> approve/reject（当前视角身份；非 admin 由控制面 403 拒绝）
//	阶段4 回写    -> watcher 终态后拉流聚合，Review 落在 PR 记录上
//
// 角色验证设计：浏览器不持任何 key； mutating 请求带 X-Demo-Role，
// 中台以对应角色凭据转发控制面——权限裁决完全在控制面，GUI 只透传结果。

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

type midway struct {
	cfg   envConfig
	cp    *cpClient
	board *board
	gh    *githubClient // GitHub 集成（未配 token 时为 nil，静态形态照常）
}

// boardView GET /api/board 的聚合视图。
type boardView struct {
	AgentMode      string            `json:"agent_mode"`   // mock | openai | down
	AgentAddr      string            `json:"agent_addr"`
	Budget         int64             `json:"daily_token_budget"`
	RateLimit      int               `json:"rate_limit_per_min"`
	AuditPath      string            `json:"audit_path"`
	Repos          []repoCat         `json:"repos"`
	Agents         map[string]string `json:"agents"` // name -> agent_id
	PRs            []pr              `json:"prs"`
	TotalTokens    int64             `json:"total_tokens"`
}

func (m *midway) getBoard(c *gin.Context) {
	m.refreshAgentHealth()
	view := boardView{
		Budget: m.cfg.dailyBudget, RateLimit: m.cfg.rateLimit,
		AuditPath: m.cfg.auditPath, Repos: m.board.repos,
		PRs: m.board.snapshot(), AgentAddr: m.cfg.agentAddr,
	}
	m.board.mu.RLock()
	view.Agents = m.board.agentIDs
	view.AgentMode = m.board.agentMode
	m.board.mu.RUnlock()
	for _, p := range view.PRs {
		if p.Review != nil {
			view.TotalTokens += p.Review.PromptTokens + p.Review.CompletionTokens
		}
	}
	c.JSON(http.StatusOK, view)
}

// refreshAgentHealth 探测裸 agent（模式展示用；不可达只降级显示，不阻塞看板）。
func (m *midway) refreshAgentHealth() {
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+m.cfg.agentAddr+"/health", nil)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		m.board.setAgentMode("down")
		return
	}
	defer resp.Body.Close()
	var out struct {
		Status string `json:"status"`
		Mode   string `json:"mode"`
		Model  string `json:"model"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&out) == nil && out.Mode != "" {
		m.board.setAgentMode(out.Mode)
	}
}

// prInput 建/webhook PR 的公共入参。
type prInput struct {
	Repo      string `json:"repo" binding:"required"`
	Title     string `json:"title"`
	PR        int    `json:"pr"`       // CI 侧已知 PR 号时透传（幂等 key 用它），缺省自增
	Workspace string `json:"workspace"` // CI 显式提供 PR 检出目录（与中台同机）
	Diff      string `json:"diff"`      // CI 显式提供补丁文本（git diff 产物）
	Agent     string `json:"agent"`     // 显式指定评审 Agent（默认按仓库分级）
}

// roleHeader 当前视角（admin | submitter | reader），缺省 admin。
func roleHeader(c *gin.Context) string {
	switch r := c.GetHeader("X-Demo-Role"); r {
	case "reader", "submitter":
		return r
	default:
		return "admin"
	}
}

// respondSubmitError 按控制面状态码语义透传（403=越权 429=治理拒绝）。
func respondSubmitError(c *gin.Context, code int, err error) {
	status := http.StatusBadRequest
	switch {
	case code == 403:
		status = http.StatusForbidden
	case code == 429:
		status = http.StatusTooManyRequests
	case code >= 500:
		status = http.StatusBadGateway
	}
	c.JSON(status, gin.H{"error": err.Error(), "code": code})
}

// createPR GUI 手动建 PR（模拟 CI 触发同一条管线，身份=当前视角）。
func (m *midway) createPR(c *gin.Context) {
	var in prInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	p, code, err := m.submitPR(c.Request.Context(), in, m.cp.keyForRole(roleHeader(c)), false)
	if err != nil {
		respondSubmitError(c, code, err)
		return
	}
	c.JSON(http.StatusCreated, p)
}

// webhookPR CI 入口（身份固定 submitter——CI 就是提交方，不是管理员）。
// 响应 202 与任务 ID；幂等由控制面 Idempotency-Key 保证（webhook 重发安全）。
func (m *midway) webhookPR(c *gin.Context) {
	var in prInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	p, code, err := m.submitPR(c.Request.Context(), in, m.cp.submitKey, false)
	if err != nil {
		respondSubmitError(c, code, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"pr": p.Number, "repo": p.Repo, "task_id": p.TaskID,
		"task_status": p.TaskStatus, "session_id": sessionID(p), "agent_id": p.AgentID,
	})
}

// submitReview 对既有 PR 重新提交评审（前次已终态时允许；排演返工场景）。
func (m *midway) submitReview(c *gin.Context) {
	p, ok := m.lookup(c)
	if !ok {
		return
	}
	if p.TaskStatus != "" && !isFinal(p.TaskStatus) && p.TaskStatus != "submit_failed" {
		c.JSON(http.StatusConflict, gin.H{"error": "task still in flight: " + p.TaskStatus})
		return
	}
	in := prInput{Repo: p.Repo, Title: p.Title, PR: p.Number}
	np, code, err := m.submitPR(c.Request.Context(), in, m.cp.keyForRole(roleHeader(c)), true)
	if err != nil {
		respondSubmitError(c, code, err)
		return
	}
	c.JSON(http.StatusAccepted, np)
}

// approve/reject 审批操作（阶段 3）。身份=当前视角；非 admin 由控制面 403 拒绝。
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
	code, err := m.cp.do(ctx, "POST", "/api/v1/tasks/"+p.TaskID+"/"+action,
		m.cp.keyForRole(roleHeader(c)), nil, &out, nil)
	if err != nil {
		status := http.StatusBadGateway
		if code == 403 {
			status = http.StatusForbidden
		}
		c.JSON(status, gin.H{"error": fmt.Sprintf("审批 %s 被拒 (%d): %v", action, code, err), "code": code})
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

// streamEvents 阶段 2 直播：PR 页 EventSource。逐行转发控制面 SSE（回放+增量）。
func (m *midway) streamEvents(c *gin.Context) {
	p, ok := m.lookup(c)
	if !ok {
		return
	}
	if p.TaskID == "" {
		c.JSON(http.StatusConflict, gin.H{"error": "pr has no task"})
		return
	}
	body, err := m.cp.stream(c.Request.Context(), p.TaskID)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	defer body.Close()

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("X-Accel-Buffering", "no")
	flusher, _ := c.Writer.(http.Flusher)
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for sc.Scan() {
		fmt.Fprintln(c.Writer, sc.Text())
		if flusher != nil {
			flusher.Flush()
		}
	}
}

// auditTail admin 专属：审计 JSONL 尾部（阶段 4 查账演示）。
// 业务数据归中台，角色门槛在中台侧；控制面动作仍由 key 裁决。
func (m *midway) auditTail(c *gin.Context) {
	if role := roleHeader(c); role != "admin" {
		c.JSON(http.StatusForbidden, gin.H{"error": "仅 admin 可查审计（当前角色 " + role + "）"})
		return
	}
	n := 20
	if _, err := fmt.Sscanf(c.DefaultQuery("n", "20"), "%d", &n); err != nil || n <= 0 || n > 200 {
		n = 20
	}
	f, err := os.Open(m.cfg.auditPath)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"entries": []any{}, "note": "审计文件尚未生成：" + m.cfg.auditPath})
		return
	}
	defer f.Close()

	lines := make([]string, 0, n)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		lines = append(lines, sc.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	entries := make([]map[string]any, 0, len(lines))
	for _, l := range lines {
		var obj map[string]any
		if json.Unmarshal([]byte(l), &obj) != nil {
			obj = map[string]any{"raw": l}
		}
		entries = append(entries, obj)
	}
	c.JSON(http.StatusOK, gin.H{"path": m.cfg.auditPath, "entries": entries})
}

// metricsProxy /metrics（admin-only）代取：浏览器不持 key，凭据留在中台进程内。
func (m *midway) metricsProxy(c *gin.Context) {
	if role := roleHeader(c); role != "admin" {
		c.JSON(http.StatusForbidden, gin.H{"error": "仅 admin 可查指标（当前角色 " + role + "）"})
		return
	}
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

// agentHealth 直查裸 agent 状态（模式/模型/连通性）。
func (m *midway) agentHealth(c *gin.Context) {
	m.refreshAgentHealth()
	m.board.mu.RLock()
	mode := m.board.agentMode
	m.board.mu.RUnlock()
	c.JSON(http.StatusOK, gin.H{"mode": mode, "addr": m.cfg.agentAddr})
}
