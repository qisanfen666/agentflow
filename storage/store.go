// Package storage 是公开扩展点②：可替换的持久化实现（M1 内存 / M2 Redis）。
// 接口按实体聚合定义；接口即合同，实现方只需满足以下行为语义。
package storage

import (
	"context"
	"errors"
	"time"

	"agentflow/model"
)

// 哨兵错误：调用方用 errors.Is 判定。
var (
	ErrNotFound        = errors.New("agentflow/storage: not found")
	ErrVersionConflict = errors.New("agentflow/storage: base version conflict")
	ErrDuplicate       = errors.New("agentflow/storage: duplicate")
)

// AgentStore 负责 AgentSpec 的存储与版本语义。
// 版本语义见 docs/contracts/agent-versioning.md：
// 版本不可变、更新即新版本（原子递增）、软删除。
type AgentStore interface {
	// Create 校验并创建 Agent，ID 与 version=1 由存储层分配。
	Create(ctx context.Context, spec model.AgentSpec) (model.AgentSpec, error)

	// Get 返回最新未删除版本；不存在或已软删除返回 ErrNotFound。
	Get(ctx context.Context, id string) (model.AgentSpec, error)

	// GetVersion 返回指定版本（含已删除 Agent 的历史，供版本锁定任务执行）。
	GetVersion(ctx context.Context, id string, version int) (model.AgentSpec, error)

	// List 返回所有未删除 Agent 的最新版本。
	List(ctx context.Context) ([]model.AgentSpec, error)

	// Update 校验 baseVersion 乐观锁后创建新版本（baseVersion+1）并返回。
	// baseVersion 落后于当前最新版本时返回 ErrVersionConflict。
	Update(ctx context.Context, id string, baseVersion int, next model.AgentSpec) (model.AgentSpec, error)

	// SoftDelete 标记 deleted_at；存量任务可继续执行（GetVersion 仍可读）。
	SoftDelete(ctx context.Context, id string) error

	// Versions 返回全部历史版本，按版本号升序。
	Versions(ctx context.Context, id string) ([]model.AgentSpec, error)
}

// TaskStore 负责任务记录的存取。M1 直接整条读写（Save 全量覆盖），
// 乐观并发控制推迟到 M2 引入 Redis 时一并设计。
type TaskStore interface {
	// Create 保存新任务，ID 与时间戳由存储层分配，status 必须为 pending。
	Create(ctx context.Context, task model.Task) (model.Task, error)

	// Get 按 ID 查询；不存在返回 ErrNotFound。
	Get(ctx context.Context, id string) (model.Task, error)

	// Save 全量覆盖保存（状态迁移后调用）。
	Save(ctx context.Context, task model.Task) error
}

// IdemStore 提交幂等原语：key -> taskID 的占位（合同 task-state.md 第 4 节）。
// M1 内存版带 TTL；M2 Redis 版 SETNX + EX，窗口期由调用方（api）配置。
type IdemStore interface {
	// PutIfAbsent 若 key 不存在（或已过期）则占位，返回 ("", true, nil)；
	// 已被占用则返回对方的 taskID 与 ("", false)。ttl<=0 视为永不过期。
	PutIfAbsent(ctx context.Context, key, taskID string, ttl time.Duration) (existing string, inserted bool, err error)
}

// ToolStore 工具注册表的持久化合同（M3）。
// v1 工具不可变、无软删除：改定义 = 删了重注册（注册表体量小，不引入版本机制）。
type ToolStore interface {
	// Create 注册工具：校验由上层（registry）完成，存储层只管唯一性——
	// Name 重复返回 ErrDuplicate。ID 与 CreatedAt 由存储层填充。
	Create(ctx context.Context, tool model.ToolDef) (model.ToolDef, error)

	// Get 按 ID 取；不存在返回 ErrNotFound。
	Get(ctx context.Context, id string) (model.ToolDef, error)

	// GetByName 按名字取（MCP 导出与 Agent 侧调用解析的入口）。
	GetByName(ctx context.Context, name string) (model.ToolDef, error)

	// List 全量列表（v1 不分页——注册表是配置量级，不是数据量级）。
	List(ctx context.Context) ([]model.ToolDef, error)
}
