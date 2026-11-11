package main

// pipeline.go —— 提交管线与任务跟踪（阶段 1 触发 / 阶段 4 回写）。
//
//	submitPR   : 建/复用 PR 记录 -> 读 diff -> 以指定角色凭据提交控制面
//	            （Idempotency-Key 按提交次数派生，webhook 重发不重复建任务）-> 起 watcher
//	watch      : 轮询任务状态；终态后一次性拉 SSE 流（Hub 历史回放保证不丢事件）
//	writeback  : 聚合 token 文本 + usage -> 提取评审结论 -> 回写 PR"评论"

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// submitPR 建 PR 并提交评审任务（GUI/webhook/CI 轮询共用）。
// key 为提交方凭据（角色验证：reader 提交会被控制面 403 拒绝）。
// resubmit：人工/CI 重新提交（前次已终态，派生新幂等 key 重跑）；
// webhook/首提交用稳定 key——CI 重发同一 PR 号拿回原任务，不重复执行。
// 返回 code 为控制面 HTTP 状态码（0=本地校验失败），供 API 层按语义透传。
func (m *midway) submitPR(ctx context.Context, in prInput, key string, resubmit bool) (*pr, int, error) {
	cat, ok := m.board.repo(in.Repo)
	if !ok {
		return nil, 400, fmt.Errorf("unknown repo %q", in.Repo)
	}
	if cat.GitHub && in.PR <= 0 {
		return nil, 400, fmt.Errorf("GitHub 仓库 %s 必须提供 pr 号", in.Repo)
	}

	// ---------- 拉取评审材料 ----------
	// GitHub 远端：PR 元数据/diff 走 API，workspace 为 head SHA 快照（zipball）。
	// 本地静态：testdata 目录 + 预置 patch（CI 产物的本地替身）。
	var (
		diff, workspace, title, headSHA, ghURL string
	)
	if cat.GitHub {
		gctx, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		meta, err := m.gh.getPR(gctx, cat.Owner, cat.Repo, in.PR)
		if err != nil {
			return nil, 502, fmt.Errorf("拉取 PR #%d 元数据: %w", in.PR, err)
		}
		headSHA = meta.HeadSHA
		ghURL = fmt.Sprintf("https://github.com/%s/%s/pull/%d", cat.Owner, cat.Repo, in.PR)
		title = in.Title
		if title == "" {
			title = meta.Title
		}
		if in.Diff != "" {
			diff = in.Diff
		} else if diff, err = m.gh.getPRDiff(gctx, cat.Owner, cat.Repo, in.PR); err != nil {
			return nil, 502, fmt.Errorf("拉取 PR #%d diff: %w", in.PR, err)
		}
		if in.Workspace != "" {
			workspace = in.Workspace
		} else if workspace, err = m.gh.ensureWorkspace(gctx, cat.Owner, cat.Repo,
			meta.HeadSHA, filepath.Join("tmp", "gh-workspaces")); err != nil {
			return nil, 502, fmt.Errorf("准备 PR #%d 工作区快照: %w", in.PR, err)
		}
	} else {
		title = in.Title
		var err error
		if in.Diff != "" {
			diff = in.Diff
		} else if diff, err = loadDiff(cat.DiffFile); err != nil {
			return nil, 400, fmt.Errorf("diff 不可读: %w", err)
		}
		workspace = cat.Workspace
		if in.Workspace != "" { // CI 显式提供检出目录（自托管 runner 共享文件系统）
			workspace = in.Workspace
		}
	}

	// CI 带 PR 号：重复触发复用既有记录（与控制面 Idempotency-Key 同一语义）
	p := m.board.findOrCreate(in.Repo, title, in.PR)

	agentName := m.board.agentFor(cat.Prod, in.Agent)
	agentID := m.board.agentID(agentName)
	if agentID == "" {
		return p, 500, fmt.Errorf("agent %q 未注册（bootstrap 未完成？）", agentName)
	}

	// 幂等 key：稳定 "pr-<repo>-<num>"（webhook 重发安全）；
	// 仅人工/CI 重新提交时派生 "-aN" 后缀（重跑场景，绕开已终态的原幂等占位）
	idemKey := sessionID(p)
	m.board.update(p.Number, func(x *pr) {
		if x.Attempts == 0 {
			x.Attempts = 1
		}
		if resubmit && x.TaskID != "" {
			x.Attempts++
			idemKey = fmt.Sprintf("%s-a%d", sessionID(x), x.Attempts)
		}
		x.Agent, x.AgentID = agentName, agentID
		if cat.GitHub {
			x.GithubURL, x.HeadSHA = ghURL, headSHA
		}
	})

	var task struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	code, err := m.cp.do(sctx, "POST", "/api/v1/tasks", key, map[string]any{
		"agent_id":   agentID,
		"session_id": sessionID(p), // 成本按 PR 归因（审计聚合维度）
		"payload": map[string]any{
			"repo": in.Repo, "pr": p.Number, "base": "main",
			"title": p.Title, "workspace": workspace, "diff": diff,
		},
		"timeout_sec": 600,
	}, &task, map[string]string{"Idempotency-Key": idemKey})
	if err != nil {
		m.board.update(p.Number, func(x *pr) { x.TaskStatus = "submit_failed"; x.TaskError = err.Error() })
		if code == 0 {
			code = 502
		}
		return p, code, fmt.Errorf("提交控制面 (%d): %w", code, err)
	}

	// 幂等重试（webhook 重发拿回原任务）不重复起 watcher。
	// 注意：p 是 board 内记录的指针别名，prev 必须在闭包内先于赋值捕获。
	var spawnWatch bool
	m.board.update(p.Number, func(x *pr) {
		spawnWatch = x.TaskID != task.ID
		x.TaskID = task.ID
		x.TaskStatus = task.Status
		x.TaskError = ""
	})
	if spawnWatch {
		go m.watch(p.Number, task.ID)
	}
	return p, 0, nil
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
	m.postReviewComment(prNum, status, rv)
}

// postReviewComment 阶段 4 回写（GitHub 形态）：评审结论发到真实 PR 评论页。
// 失败不影响本地结果（rv.PostedToGithub 记录原因，GUI 可见）。
func (m *midway) postReviewComment(prNum int, status string, rv *review) {
	if m.gh == nil || !m.cfg.ghPostComments || status != "succeeded" || rv.Markdown == "" {
		return
	}
	p, ok := m.board.get(prNum)
	if !ok {
		return
	}
	cat, ok := m.board.repo(p.Repo)
	if !ok || !cat.GitHub {
		return
	}
	body := fmt.Sprintf("## agentflow 自动评审\n\n**结论**：%s · token %d+%d\n\n%s\n\n> 由 pr-review-new demo（agentflow 控制面 + 裸 agent）自动评审产生",
		rv.Conclusion, rv.PromptTokens, rv.CompletionTokens, rv.Markdown)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := m.gh.postComment(ctx, cat.Owner, cat.Repo, prNum, body); err != nil {
		rv.PostedToGithub = "failed: " + err.Error()
		log.Printf("[github] PR #%d 评论回写失败: %v", prNum, err)
		return
	}
	rv.PostedToGithub = "ok"
	log.Printf("[github] PR #%d 评审结论已回写评论", prNum)
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

func sessionID(p *pr) string { return fmt.Sprintf("pr-%s-%d", p.Repo, p.Number) }

// isFinal 任务终态判定（与控制面 task-state 合同一致；中台只镜像状态字符串）。
func isFinal(status string) bool {
	switch status {
	case "succeeded", "failed", "cancelled", "timeout":
		return true
	}
	return false
}
