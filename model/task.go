package model

import (
	"fmt"
	"time"
)

// TaskStatus 任务状态。语义与迁移表见 docs/contracts/task-state.md。
type TaskStatus string

const (
	TaskPending         TaskStatus = "pending"
	TaskPendingApproval TaskStatus = "pending_approval" // 高危 Agent 的任务提交后先待审，未入队
	TaskRunning         TaskStatus = "running"
	TaskSucceeded       TaskStatus = "succeeded"
	TaskFailed          TaskStatus = "failed"
	TaskCancelled       TaskStatus = "cancelled"
	TaskTimeout         TaskStatus = "timeout"
)

// finalStates 终态集合：进入后不可再迁移。
var finalStates = map[TaskStatus]bool{
	TaskSucceeded: true,
	TaskFailed:    true,
	TaskCancelled: true,
	TaskTimeout:   true,
}

// transitions 合法迁移表，与合同文档的迁移表一一对应：
//
//	pending          -> running, pending_approval, cancelled
//	pending_approval -> pending(审批通过), cancelled(驳回/取消)
//	running          -> succeeded, failed, timeout, pending(重试), cancelled
var transitions = map[TaskStatus]map[TaskStatus]bool{
	TaskPending: {TaskRunning: true, TaskPendingApproval: true, TaskCancelled: true},
	TaskPendingApproval: {
		TaskPending:   true, // approve：放行入队
		TaskCancelled: true, // reject 或用户取消
	},
	TaskRunning: {
		TaskSucceeded: true,
		TaskFailed:    true,
		TaskTimeout:   true,
		TaskPending:   true,
		TaskCancelled: true,
	},
}

// Final 报告 s 是否为终态。
func (s TaskStatus) Final() bool { return finalStates[s] }

// CanTransition 报告 from -> to 是否合法。
func CanTransition(from, to TaskStatus) bool {
	return transitions[from][to]
}

// TaskError 是跨网络传输的错误表示，只传 code + message。
type TaskError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Usage 是 usage 事件的累加结果（合同：多次出现则求和）。
type Usage struct {
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	Model            string `json:"model,omitempty"`
}

// Task 是一次任务执行的完整记录。
// 创建时锁定 agent_version，执行按锁定版本路由（版本锁定语义）。
type Task struct {
	ID           string         `json:"id"`
	AgentID      string         `json:"agent_id"`
	AgentVersion int            `json:"agent_version"`
	SessionID    string         `json:"session_id,omitempty"`    // 可选归因分组：同会话任务的聚合维度（成本/审计）。控制面不理解会话语义
	TraceContext string         `json:"trace_context,omitempty"` // 提交侧 span 的 traceparent：跨队列的因果载体（worker 侧以 Link 挂回）
	Status       TaskStatus     `json:"status"`
	Payload      map[string]any `json:"payload,omitempty"`
	TimeoutSec   int            `json:"timeout_sec,omitempty"`
	AttemptCount int            `json:"attempt_count"`
	Error        *TaskError     `json:"error,omitempty"`
	Usage        *Usage         `json:"usage,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

// Transition 校验并执行状态迁移。
// 合同要求：对终态/非法路径的迁移必须拒绝并记录，而非静默覆盖。
func (t *Task) Transition(to TaskStatus) error {
	if !CanTransition(t.Status, to) {
		return fmt.Errorf("invalid transition %s -> %s (task %s)", t.Status, to, t.ID)
	}
	// attempt 语义：每次真正开始执行（pending -> running）+1，首跑即为 1
	if t.Status == TaskPending && to == TaskRunning {
		t.AttemptCount++
	}
	t.Status = to
	t.UpdatedAt = time.Now()
	return nil
}
