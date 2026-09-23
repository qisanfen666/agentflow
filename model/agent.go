// Package model 定义控制面的领域类型。
// 本包是全项目状态常量与错误码的单一来源，保持零外部依赖。
package model

import (
	"errors"
	"fmt"
	"time"
)

// RuntimeType 标识 Agent 的执行环境类型，控制面据此路由到对应 Runtime 实现。
const (
	RuntimePythonHTTP = "python-http" // HTTP 直连 Agent
	RuntimeDocker     = "docker"      // Docker 沙箱
)

// RuntimeSpec 描述执行环境路由信息，按 Type 分派到对应 Runtime。
type RuntimeSpec struct {
	Type  string            `json:"type"`            // RuntimeType 之一
	Host  string            `json:"host,omitempty"`  // python-http 必填，Agent 服务地址
	Image string            `json:"image,omitempty"` // docker 可选，缺省用沙箱默认镜像
	Env   map[string]string `json:"env,omitempty"`   // 注入环境变量
}

// AgentSpec 是一个 Agent 定义的不可变快照。
// 语义见 docs/contracts/agent-versioning.md：
// (id, version) 一经创建不可修改，更新即创建新版本；删除为软删除。
type AgentSpec struct {
	ID      string         `json:"id"`
	Name    string         `json:"name"`
	Type    string         `json:"type"` // 业务类型标签（chat / rag / workflow...）
	Runtime RuntimeSpec    `json:"runtime"`
	Config  map[string]any `json:"config,omitempty"` // 原样透传给 Agent 的自定义配置
	// RequireApproval 管理员声明此 Agent 高危：其任务提交后进入 pending_approval，
	// admin 审批通过才入队。风险决策权在管理端（spec 经乐观锁变更、审计留痕），
	// 不交给调用方——真正危险的调用者不会自审。
	RequireApproval bool       `json:"require_approval,omitempty"`
	Version         int        `json:"version"` // 从 1 开始单调递增
	CreatedAt       time.Time  `json:"created_at"`
	DeletedAt       *time.Time `json:"deleted_at,omitempty"` // 软删除标记，nil = 未删除
}

// Validate 校验 spec 的必填性约束，在创建与更新时调用。
func (a *AgentSpec) Validate() error {
	if a.Name == "" {
		return errors.New("name is required")
	}
	if a.Type == "" {
		return errors.New("type is required")
	}
	switch a.Runtime.Type {
	case RuntimePythonHTTP:
		if a.Runtime.Host == "" {
			return errors.New("runtime.host is required for python-http")
		}
	case RuntimeDocker:
		// image 可选，运行时用沙箱默认镜像
	default:
		return fmt.Errorf("unsupported runtime type: %q", a.Runtime.Type)
	}
	return nil
}

// Deleted 报告该 Agent 是否已被软删除。
func (a *AgentSpec) Deleted() bool { return a.DeletedAt != nil }
