// watcher.go —— 提交管线与任务跟踪。
//
//	submitPR   : 建 PR 记录 -> 读 diff -> 提交控制面（Idempotency-Key=PR 号）-> 起 watcher
//	watcher    : 轮询任务状态；终态后一次性拉 SSE 流（Hub 历史回放保证不丢事件）
//	            聚合 token 文本 + usage -> 生成 Review"评论"回写 PR
//	streamEvents: SSE 直播代理（PR 页实时看 claude 读文件/输出）
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// submitPR 建 PR 并提交评审任务（GUI 与 webhook 共用）。
func (m *midway) submitPR(ctx context.Context, in prInput) (*pr, error) {
	cat, ok := m.board.repo(in.Repo)
	if !ok {
		return nil, fmt.Errorf("unknown repo %q", in.Repo)
	}
	diff := in.Diff
	if diff == "" {
		var err error
		diff, err = loadDiff(cat.DiffFile)
		if err != nil {
			return nil, fmt.Errorf("diff 不可读（先跑 examples/pr-review/scripts/make-testdata.ps1）: %w", err)
		}
	}
	workspace := cat.Workspace
	if in.Workspace != "" { // CI 显式提供检出目录（自托管 runner 共享文件系统）
		workspace = in.Workspace
	}

	// CI 带 PR 号：重复触发复用既有记录（与控制面 Idempotency-Key 同一语义）
	p := m.board.findOrCreate(in.Repo, in.Title, in.PR)

	agentName := m.board.reviewerFor(cat.Prod)
	m.board.mu.RLock()
	agentID := m.board.agentIDs[agentName]
	m.board.mu.RUnlock()
	m.board.update(p.Number, func(x *pr) { x.AgentID = agentID })

	// 控制面幂等：同一 PR 号重复 webhook 只会拿到原任务
	var task struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	code, err := m.cp.do(sctx, "POST", "/api/v1/tasks", m.cp.submitKey, map[string]any{
		"agent_id":   agentID,
		"session_id": sessionID(p), // 成本按 PR 归因（审计聚合维度）
		"payload": map[string]any{
			"repo": in.Repo, "pr": p.Number, "base": "main",
			"title": p.Title, "workspace": workspace, "diff": diff,
		},
		"timeout_sec": 600,
	}, &task, map[string]string{"Idempotency-Key": fmt.Sprintf("pr-%s-%d", in.Repo, p.Number)})
	if err != nil {
		m.board.update(p.Number, func(x *pr) { x.TaskStatus = "submit_failed"; x.TaskError = err.Error() })
		return nil, fmt.Errorf("提交控制面 (%d): %w", code, err)
	}

	m.board.update(p.Number, func(x *pr) {
		x.TaskID = task.ID
		x.TaskStatus = task.Status
	})
	go m.watch(p.Number, task.ID)
	return p, nil
}

// watch 跟踪任务到终态并回写。轮询频率对 demo 足够；生产可换事件订阅。
func (m *midway) watch(prNum int, taskID string) {
	ctx := context.Background()
	deadline := time.Now().Add(15 * time.Minute)

	for {
		var task struct {
			Status string `json:"status"`
			Error  *struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if _, err := m.cp.do(ctx, "GET", "/api/v1/tasks/"+taskID, m.cp.readerKey, nil, &task, nil); err == nil {
			m.board.update(prNum, func(x *pr) {
				x.TaskStatus = task.Status
				if task.Error != nil {
					x.TaskError = task.Error.Code + ": " + task.Error.Message
				}
			})
			if isFinal(task.Status) {
				m.writeback(prNum, taskID, task.Status)
				return
			}
		}
		if time.Now().After(deadline) {
			m.board.update(prNum, func(x *pr) { x.TaskStatus = "watch_timeout" })
			return
		}
		time.Sleep(700 * time.Millisecond)
	}
}

// writeback 阶段 4：终态后拉全量事件流（Hub 回放），聚合评审结论回写 PR。
func (m *midway) writeback(prNum int, taskID, status string) {
	rv := &review{FinishedAt: time.Now(), Conclusion: "FAILED"}
	defer func() {
		m.board.update(prNum, func(x *pr) { x.Review = rv })
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := m.cp.stream(ctx, taskID)
	if err != nil {
		rv.Markdown = "> 回写失败：拉取事件流失败 " + err.Error()
		return
	}
	defer body.Close()

	var sb strings.Builder
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		var ev struct {
			Type             string `json:"type"`
			Content          string `json:"content"`
			Code             string `json:"code"`
			Message          string `json:"message"`
			PromptTokens     int64  `json:"prompt_tokens"`
			CompletionTokens int64  `json:"completion_tokens"`
		}
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev) != nil {
			continue
		}
		switch ev.Type {
		case "token":
			sb.WriteString(ev.Content)
		case "usage":
			rv.PromptTokens += ev.PromptTokens
			rv.CompletionTokens += ev.CompletionTokens
		case "error":
			rv.Markdown = fmt.Sprintf("> 评审失败（%s）：%s\n\n%s", ev.Code, ev.Message, sb.String())
			return
		}
	}
	rv.Markdown = sb.String()
	if status == "succeeded" {
		rv.Conclusion = extractConclusion(sb.String())
	} else {
		rv.Conclusion = "FAILED"
	}
}

var conclusionRe = regexp.MustCompile(`\b(APPROVE|REQUEST_CHANGES)\b`)

// extractConclusion 从评审文本提取结论（prompt 约定最后一行给结论）。
func extractConclusion(text string) string {
	all := conclusionRe.FindAllString(text, -1)
	if len(all) == 0 {
		return "UNKNOWN"
	}
	return all[len(all)-1]
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

// isFinal 任务终态判定（与控制面 task-state 合同一致；中台只镜像状态字符串）。
func isFinal(status string) bool {
	switch status {
	case "succeeded", "failed", "cancelled", "timeout":
		return true
	}
	return false
}
