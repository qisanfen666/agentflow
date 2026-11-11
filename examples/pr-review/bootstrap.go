package main

// bootstrap.go —— 阶段 0：管理员一次性初始化（幂等）。
//
// 三个评审 Agent（对照故事的角色模板）：
//
//	pr-reviewer       只读工具集（Read/Grep/Glob），普通仓库，免审批
//	pr-reviewer-prod  只读工具集，生产仓库，任务级审批（require_approval）
//	auto-fixer        +Edit/Bash，可写 workspace，高危：审批 + 一次性容器兜底
//
// 工具白名单不是控制面概念：经 AgentSpec.Config 透传，由 claude Runtime
// 翻译成 --allowedTools（风险决策在管理端 spec，调用方无法自选）。

import (
	"context"
	"fmt"
	"log"
	"os"
)

const (
	agentReviewer    = "pr-reviewer"
	agentReviewerProd = "pr-reviewer-prod"
	agentAutoFixer   = "auto-fixer"
)

type agentSpecInput struct {
	Name            string         `json:"name"`
	Type            string         `json:"type"`
	Runtime         runtimeSpecIn  `json:"runtime"`
	Config          map[string]any `json:"config"`
	RequireApproval bool           `json:"require_approval"`
}

type runtimeSpecIn struct {
	Type string            `json:"type"`
	Env  map[string]string `json:"env,omitempty"`
}

func (m *midway) bootstrapAgents(ctx context.Context) error {
	// 认证/端点透传：任一存在才注入 spec.Env（容器模式 -e 进容器；本地模式并入子进程 env）。
	//   ANTHROPIC_API_KEY    官方 API key 或中转站 key（跳过 claude login 的标准姿势）
	//   ANTHROPIC_BASE_URL   Anthropic 兼容网关（官方可省）
	//   ANTHROPIC_AUTH_TOKEN  Bearer 型网关（少数中转用）
	keyEnv := map[string]string{}
	for _, k := range []string{"ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "ANTHROPIC_AUTH_TOKEN"} {
		if v := os.Getenv(k); v != "" {
			keyEnv[k] = v
		}
	}
	readOnly := []any{"Read", "Grep", "Glob"}
	fixTools := []any{"Read", "Grep", "Glob", "Edit", "Bash"}

	specs := []agentSpecInput{
		{
			Name: agentReviewer, Type: "review",
			Runtime: runtimeSpecIn{Type: "claude-code", Env: keyEnv},
			Config: map[string]any{
				"allowed_tools": readOnly, "workspace_readonly": true, "max_turns": 12,
			},
		},
		{
			Name: agentReviewerProd, Type: "review",
			Runtime: runtimeSpecIn{Type: "claude-code", Env: keyEnv},
			Config: map[string]any{
				"allowed_tools": readOnly, "workspace_readonly": true, "max_turns": 12,
			},
			RequireApproval: true, // 生产仓库：任务级审批闸门
		},
		{
			Name: agentAutoFixer, Type: "fix",
			Runtime: runtimeSpecIn{Type: "claude-code", Env: keyEnv},
			Config: map[string]any{
				"allowed_tools": fixTools, "workspace_readonly": false, "max_turns": 16,
			},
			RequireApproval: true, // 高危：可写 + Bash
		},
	}

	for _, spec := range specs {
		if err := m.ensureAgent(ctx, spec); err != nil {
			return fmt.Errorf("bootstrap %s: %w", spec.Name, err)
		}
	}
	log.Printf("bootstrap: agents ready %v", m.board.agentIDs)
	return nil
}

// ensureAgent 幂等注册：按名字找，无则建。已存在不改（版本管理归管理员，
// 走 PUT + base_version 乐观锁，demo 不自动变更生产配置）。
func (m *midway) ensureAgent(ctx context.Context, in agentSpecInput) error {
	var list []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if _, err := m.cp.do(ctx, "GET", "/api/v1/agents", m.cp.adminKey, nil, &list, nil); err != nil {
		return err
	}
	for _, a := range list {
		if a.Name == in.Name {
			m.board.mu.Lock()
			m.board.agentIDs[in.Name] = a.ID
			m.board.mu.Unlock()
			return nil
		}
	}
	var created struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if code, err := m.cp.do(ctx, "POST", "/api/v1/agents", m.cp.adminKey, in, &created, nil); err != nil {
		return fmt.Errorf("create (%d): %w", code, err)
	}
	m.board.mu.Lock()
	m.board.agentIDs[in.Name] = created.ID
	m.board.mu.Unlock()
	return nil
}

// reviewerFor 生产仓库走 -prod Agent；auto-fixer 属后续场景，预留选择。
func (b *board) reviewerFor(prod bool) string {
	if prod {
		return agentReviewerProd
	}
	return agentReviewer
}
