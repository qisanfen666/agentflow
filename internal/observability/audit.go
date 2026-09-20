// Package observability 提供审计日志原语（M5 起：metrics / tracing 将陆续入驻）。
// 零内部依赖，处于依赖链最底层，可被 api / dispatch / facade 任意引用。
package observability

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"
)

// 审计动作与实体常量。集中定义避免字符串散落各埋点处。
const (
	EntityAgent = "agent"
	EntityTask  = "task"
	EntityTool  = "tool"

	ActionAgentCreated   = "agent_created"
	ActionAgentUpdated   = "agent_updated"
	ActionAgentDeleted   = "agent_deleted"
	ActionTaskSubmitted  = "task_submitted"
	ActionTaskCancelled  = "task_cancelled"
	ActionTaskStarted    = "task_started"
	ActionTaskRetry      = "task_retry"
	ActionTaskSucceeded  = "task_succeeded"
	ActionTaskFailed     = "task_failed"
	ActionTaskTimeout    = "task_timeout"
	ActionToolRegistered = "tool_registered"
)

// AuditEvent 一条审计记录：谁（实体）在何时发生了什么动作。
// Detail 携带动作语义相关的补充字段（error_code / attempt / usage 等），
// 不承载业务负载，敏感字段由埋点方负责脱敏。
type AuditEvent struct {
	Time     time.Time      `json:"time"` // 零值时由 logger 补当前时间
	Action   string         `json:"action"`
	Entity   string         `json:"entity"`
	EntityID string         `json:"entity_id"`
	Detail   map[string]any `json:"detail,omitempty"`
}

// AuditLogger 审计接收端。实现方可替换：文件、集中式审计服务、测试收集器。
type AuditLogger interface {
	Record(ctx context.Context, ev AuditEvent) error
}

// Telemetry 聚合观测与治理回报组件，随组件构造注入。字段均可为零值（nil = 未启用），
// 埋点路径全部 nil-safe。
type Telemetry struct {
	Audit   AuditLogger
	Metrics *Metrics
	Budget  BudgetRecorder // 成本护栏回报接口（实现：policy.BudgetTracker）
}

// BudgetRecorder 成本护栏的回报面：dispatcher 只依赖这个最小接口，
// 具体计数策略（内存日预算/分布式）由装配方决定。
type BudgetRecorder interface {
	Record(prompt, completion int64)
}

// RecordBestEffort 尽力审计：写失败只记日志，不阻断业务主流程——
// 审计是旁路观测，不能因它把任务链路打挂。nil 容忍（未配置即关闭）。
func RecordBestEffort(l AuditLogger, ctx context.Context, ev AuditEvent) {
	if l == nil {
		return
	}
	if err := l.Record(ctx, ev); err != nil {
		log.Printf("[audit] %s %s/%s: %v", ev.Action, ev.Entity, ev.EntityID, err)
	}
}

// FileAuditLogger append-only JSONL 文件审计：每行一条 JSON，只追加不修改不删除。
// 为什么是文件不是数据库：审计的查询需求归宿主的日志系统（ELK 等），
// 控制面只负责按序、不可变地落盘；append 语义由 O_APPEND 提供文件系统级保证。
type FileAuditLogger struct {
	mu sync.Mutex
	f  *os.File
}

// NewFileAuditLogger 打开（或创建）审计文件并定位到末尾。
// 打不开立即报错——审计配置指向坏路径应属于启动失败，而非运行期静默丢审计。
func NewFileAuditLogger(path string) (*FileAuditLogger, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileAuditLogger{f: f}, nil
}

// Record 序列化事件并追加一行。互斥锁串行化保证行完整性（无交错断行）。
func (l *FileAuditLogger) Record(_ context.Context, ev AuditEvent) error {
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err = l.f.Write(append(data, '\n'))
	return err
}

// Close 关闭底层文件（Panel.Stop 时调用）。
func (l *FileAuditLogger) Close() error { return l.f.Close() }
