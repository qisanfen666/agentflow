package main

// bootstrap.go —— 阶段 0：管理员一次性初始化（幂等）。
//
// 三个评审 Agent（对照故事的角色模板）。执行面是同一个裸 agent 进程
//（python-http 直连），风险差异全部声明在 spec 里：
//
//	pr-reviewer       read_file 只读，普通仓库，免审批
//	pr-reviewer-prod  read_file 只读，生产仓库，任务级审批（require_approval）
//	auto-fixer        +write_file 可写工作区，高危：审批兜底
//
// 工具白名单不是控制面概念：经 AgentSpec.Config 透传给 agent，
// 由 agent 在工具循环里强制（风险决策在管理端 spec，调用方无法自选）。

import (
	"context"
	"fmt"
	"log"
)

const (
	agentReviewer     = "pr-reviewer"
	agentReviewerProd = "pr-reviewer-prod"
	agentAutoFixer    = "auto-fixer"
)

type agentSpecInput struct {
	Name            string         `json:"name"`
	Type            string         `json:"type"`
	Runtime         runtimeSpecIn  `json:"runtime"`
	Config          map[string]any `json:"config"`
	RequireApproval bool           `json:"require_approval"`
}

type runtimeSpecIn struct {
	Type string `json:"type"`
	Host string `json:"host"`
}

func (m *midway) bootstrapAgents(ctx context.Context) error {
	host := "http://" + m.cfg.agentAddr
	readOnly := []any{"read_file"}
	fixTools := []any{"read_file", "write_file"}

	specs := []agentSpecInput{
		{
			Name: agentReviewer, Type: "review",
			Runtime: runtimeSpecIn{Type: "python-http", Host: host},
			Config: map[string]any{"allowed_tools": readOnly, "max_tool_rounds": 6},
		},
		{
			Name: agentReviewerProd, Type: "review",
			Runtime: runtimeSpecIn{Type: "python-http", Host: host},
			Config: map[string]any{"allowed_tools": readOnly, "max_tool_rounds": 6},
			RequireApproval: true, // 生产仓库：任务级审批闸门
		},
		{
			Name: agentAutoFixer, Type: "fix",
			Runtime: runtimeSpecIn{Type: "python-http", Host: host},
			Config: map[string]any{"allowed_tools": fixTools, "max_tool_rounds": 8},
			RequireApproval: true, // 高危：可写工作区
		},
	}

	for _, spec := range specs {
		if err := m.ensureAgent(ctx, spec); err != nil {
			return fmt.Errorf("bootstrap %s: %w", spec.Name, err)
		}
	}
	log.Printf("bootstrap: agents ready %v (agent host %s)", m.board.agentIDs, host)
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
