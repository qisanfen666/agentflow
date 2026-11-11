package main

// ci.go —— CI 自动触发：轮询 GitHub 开放 PR，新 PR 自动评审、新提交自动重跑。
//
// 为什么是轮询而不是 GitHub webhook/Actions 直推：中台跑在本机（localhost），
// GitHub 的服务器无法主动连进来；轮询是同一事件语义（PR 创建/更新 → 自动评审）
// 的本地可行实现。中台部署到公网后可平替为 webhook（POST /webhook/pr 已就绪），
// 管线完全复用（都走 submitPR）。
//
// 防误评审：branch_prefix 只自动评审匹配前缀的分支（如 "drill/"），
// 真实工作 PR 不受影响；GUI/接口手动提交不受此限。

import (
	"context"
	"log"
	"strings"
	"time"
)

func (m *midway) runCIPoller(ctx context.Context) {
	interval := time.Duration(m.cfg.ciPollSec) * time.Second
	if m.gh == nil || m.gh.token == "" {
		log.Printf("[ci] GitHub token 未配置，自动评审关闭（手动提交不受影响）")
		return
	}
	if interval <= 0 {
		log.Printf("[ci] poll_sec=0，自动评审关闭")
		return
	}
	var repos []repoCat
	for _, cat := range m.board.repos {
		if cat.GitHub {
			repos = append(repos, cat)
		}
	}
	if len(repos) == 0 {
		log.Printf("[ci] 无 GitHub 远端仓库，自动评审关闭")
		return
	}
	log.Printf("[ci] 自动评审已启动：每 %s 轮询 %d 个 GitHub 仓库（branch_prefix=%q）",
		interval, len(repos), m.cfg.ghBranchPrefix)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	m.ciSweep(ctx, repos) // 启动即扫一轮（存量 PR 不等第一个 tick）
	for {
		select {
		case <-ctx.Done():
			log.Printf("[ci] 退出")
			return
		case <-ticker.C:
			m.ciSweep(ctx, repos)
		}
	}
}

func (m *midway) ciSweep(ctx context.Context, repos []repoCat) {
	for _, cat := range repos {
		sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		prs, err := m.gh.listOpenPRs(sctx, cat.Owner, cat.Repo)
		cancel()
		if err != nil {
			log.Printf("[ci] %s 列表失败: %v", cat.Name, err)
			continue
		}
		for _, gp := range prs {
			m.ciConsider(ctx, cat, gp)
		}
	}
}

func (m *midway) ciConsider(ctx context.Context, cat repoCat, gp ghPR) {
	if m.cfg.ghBranchPrefix != "" && !strings.HasPrefix(gp.HeadRef, m.cfg.ghBranchPrefix) {
		return // 非演练分支：不动用户的真实 PR
	}
	existing, ok := m.board.getByRepo(cat.Name, gp.Number)
	if !ok {
		// 新 PR：自动评审（CI 语义：以 submitter 身份走同一条管线）
		log.Printf("[ci] %s PR #%d（%s）新 PR -> 自动提交评审", cat.Name, gp.Number, gp.HeadRef)
		m.ciSubmit(ctx, cat, gp.Number, false)
		return
	}
	// 已有记录：head 变了且前次已终态 -> 自动重跑（幂等 key 派生见 submitPR）
	if existing.HeadSHA != "" && existing.HeadSHA != gp.HeadSHA && isFinal(existing.TaskStatus) {
		log.Printf("[ci] %s PR #%d 有新提交 %s -> 自动重新评审", cat.Name, gp.Number, gp.HeadSHA[:8])
		m.ciSubmit(ctx, cat, gp.Number, true)
	}
}

func (m *midway) ciSubmit(ctx context.Context, cat repoCat, num int, resubmit bool) {
	sctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	_, _, err := m.submitPR(sctx, prInput{Repo: cat.Name, PR: num}, m.cp.submitKey, resubmit)
	if err != nil {
		log.Printf("[ci] %s PR #%d 提交失败: %v", cat.Name, num, err)
	}
}
