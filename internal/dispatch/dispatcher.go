// Package dispatch 实现任务执行编排：按锁定版本解析 spec、路由到 Runtime、
// 驱动任务状态机、发布事件到 Hub。
// M2 起 Execute 为同步方法，由 engine.Worker 从队列取出后调用；
// 重试决策（Nack 退避 or 终态）由 worker 依据返回值执行。
package dispatch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"agentflow/model"
	"agentflow/runtime"
	"agentflow/storage"
)

// DefaultMaxRetries 可重试错误的默认最大重试次数（首跑 + 3 次重试）。
const DefaultMaxRetries = 3

// DefaultTimeoutSec 任务级默认执行超时（秒），对应 OpenAPI TaskInput.timeout_sec 默认值。
const DefaultTimeoutSec = 300

// RetryableError 表示任务失败但满足重试条件：任务已回退为 pending 并持久化，
// 调用方（worker）应 Nack 延迟退避后重新消费。
type RetryableError struct {
	Code    string // 触发重试的错误码（model 常量）
	Attempt int    // 刚结束的这次是第几次尝试（退避计算用）
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable failure: %s (attempt %d)", e.Code, e.Attempt)
}

// ---------- EventHub：每任务的事件总线 ----------

// taskStream 单任务的事件流：append-only 历史 + 完成标志。
// notify 为容量 1 的信号通道：追加事件时非阻塞发送（天然合并重复唤醒），
// Finish 时 close——close 后所有接收立即返回，订阅循环据此自然终止。
type taskStream struct {
	mu       sync.Mutex
	events   []runtime.Event
	finished bool
	notify   chan struct{}
}

func (s *taskStream) publish(ev runtime.Event) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default: // 已有 pending 唤醒，合并
	}
}

func (s *taskStream) finish() {
	s.mu.Lock()
	s.finished = true
	s.mu.Unlock()
	close(s.notify)
}

// Hub 是全部任务事件流的总线。M1 事件只存内存（demo 级）；M5 可观测性阶段再考虑持久化。
// 注意：任务重试时事件继续追加到同一条流（订阅者看到的是各次尝试的拼接）。
type Hub struct {
	mu      sync.Mutex
	streams map[string]*taskStream
}

func NewHub() *Hub { return &Hub{streams: make(map[string]*taskStream)} }

func (h *Hub) streamFor(taskID string) *taskStream {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.streams[taskID]
	if !ok {
		s = &taskStream{notify: make(chan struct{}, 1)}
		h.streams[taskID] = s
	}
	return s
}

// Publish 追加事件。约束：单写者（该任务的分发 goroutine），Finish 后不得再 Publish。
func (h *Hub) Publish(taskID string, ev runtime.Event) {
	h.streamFor(taskID).publish(ev)
}

// Finish 结束事件流（终态已定）。
func (h *Hub) Finish(taskID string) {
	h.streamFor(taskID).finish()
}

// Since 返回从下标 i 开始的事件副本与完成标志；任务从未 Publish 过则 ok=false。
func (h *Hub) Since(taskID string, i int) (evs []runtime.Event, finished bool, ok bool) {
	h.mu.Lock()
	s, exist := h.streams[taskID]
	h.mu.Unlock()
	if !exist {
		return nil, false, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if i > len(s.events) {
		i = len(s.events)
	}
	evs = make([]runtime.Event, len(s.events)-i)
	copy(evs, s.events[i:])
	return evs, s.finished, true
}

// Notify 返回该任务流的唤醒通道（有新事件或已结束时可读）。
func (h *Hub) Notify(taskID string) <-chan struct{} {
	return h.streamFor(taskID).notify
}

// ---------- Dispatcher：执行编排器 ----------

// Dispatcher 按锁定版本解析 spec、路由到 Runtime、驱动任务状态机、发布事件。
type Dispatcher struct {
	runtimes map[string]runtime.Runtime // key: Runtime.Type()
	agents   storage.AgentStore
	tasks    storage.TaskStore
	hub      *Hub

	// MaxRetries 可重试错误的次数上限；超过则落终态。
	// 默认 DefaultMaxRetries，wiring 时由 queue 配置覆盖。
	MaxRetries int

	mu      sync.Mutex
	cancels map[string]context.CancelFunc // 运行中任务的取消句柄
}

// New 创建分发器，rts 至少要有一个；重复 Type() 以后注册者胜。
func New(agents storage.AgentStore, tasks storage.TaskStore, hub *Hub, rts ...runtime.Runtime) *Dispatcher {
	d := &Dispatcher{
		runtimes:   make(map[string]runtime.Runtime),
		agents:     agents,
		tasks:      tasks,
		hub:        hub,
		MaxRetries: DefaultMaxRetries,
		cancels:    make(map[string]context.CancelFunc),
	}
	for _, rt := range rts {
		d.runtimes[rt.Type()] = rt
	}
	return d
}

// Execute 同步执行任务（worker 从队列取出后调用）。
//
// 返回值语义（worker 的 Ack/Nack 依据）：
//   - nil：终态已定并持久化（succeeded/failed/timeout/cancelled）-> Ack
//   - *RetryableError：任务已回退 pending 并持久化 -> Nack(退避)
//   - 其他 error：控制面内部故障，任务状态未定 -> Nack(0) 交由可见性超时兜底
//
// 若任务已被并发取消（cancelled 为终态），Transition 拒绝，本方法直接返回。
func (d *Dispatcher) Execute(task model.Task) error {
	ctx := context.Background()
	if err := task.Transition(model.TaskRunning); err != nil {
		// 已被取消等：终态优先，直接放弃执行。必须关流，否则订阅者永久挂起。
		d.hub.Finish(task.ID)
		return nil
	}
	if err := d.tasks.Save(ctx, task); err != nil {
		d.hub.Finish(task.ID)
		return fmt.Errorf("save running task: %w", err)
	}

	spec, err := d.agents.GetVersion(ctx, task.AgentID, task.AgentVersion)
	if err != nil {
		return d.finalize(&task, &model.TaskError{Code: model.ErrInvalidPayload, Message: "agent version not found"})
	}
	rt, ok := d.runtimes[spec.Runtime.Type]
	if !ok {
		return d.finalize(&task, &model.TaskError{Code: model.ErrInternal, Message: "no runtime registered for type " + spec.Runtime.Type})
	}

	timeout := time.Duration(task.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = DefaultTimeoutSec * time.Second
	}
	// 注意：脱离 HTTP 请求 ctx——任务生命周期独立于提交请求
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	d.register(task.ID, cancel)
	defer d.unregister(task.ID)

	for ev := range rt.Execute(runCtx, runtime.Request{
		TaskID:      task.ID,
		Spec:        spec.Runtime,
		AgentConfig: spec.Config,
		Payload:     task.Payload,
	}) {
		d.hub.Publish(task.ID, ev)

		switch ev.Type {
		case runtime.EventUsage:
			if task.Usage == nil {
				task.Usage = &model.Usage{}
			}
			task.Usage.PromptTokens += ev.PromptTokens
			task.Usage.CompletionTokens += ev.CompletionTokens
			if ev.Model != "" {
				task.Usage.Model = ev.Model
			}
		case runtime.EventDone:
			_ = task.Transition(model.TaskSucceeded) // 已取消时拒绝，忽略
		case runtime.EventError:
			task.Error = &model.TaskError{Code: ev.Code, Message: ev.Message}
			if model.IsRetryable(ev.Code) && task.AttemptCount <= d.MaxRetries {
				// 可重试且还有名额：回退 pending 交给 worker 重新入队。
				// 注意流不 Finish——重试的事件会继续追加到同一条流；
				// defer cancel() 会终止 runCtx，Runtime 侧 goroutine 随之收尾，不泄漏。
				if err := task.Transition(model.TaskPending); err == nil {
					if err := d.tasks.Save(ctx, task); err != nil {
						return fmt.Errorf("save retryable task: %w", err)
					}
					return &RetryableError{Code: ev.Code, Attempt: task.AttemptCount}
				}
				// 迁移失败（已被取消）：落入终态路径
			} else {
				// 不可重试，或重试名额耗尽：终态。
				// AGENT_TIMEOUT 单独落 timeout（环境故障率需要独立监控指标）。
				if ev.Code == model.ErrAgentTimeout {
					_ = task.Transition(model.TaskTimeout)
				} else {
					_ = task.Transition(model.TaskFailed)
				}
			}
		}
	}
	// 通道关闭但任务仍非终态：Runtime 违反"错误即事件"契约（如已被取消）
	if !task.Status.Final() {
		return d.finalize(&task, &model.TaskError{Code: model.ErrAgentProtocolViolation, Message: "runtime closed stream without terminal event"})
	}
	return d.finalize(&task, nil)
}

// finalize 落终态并关闭事件流。te 非 nil 时（内部失败路径）补发一个 error 事件再结束，
// 保证订阅者视角"必有终止信号"。返回 nil（终态即成功处理的 Ack 依据）或内部错误。
func (d *Dispatcher) finalize(task *model.Task, te *model.TaskError) error {
	if te != nil {
		task.Error = te
		_ = task.Transition(model.TaskFailed)
		d.hub.Publish(task.ID, runtime.Event{Type: runtime.EventError, Code: te.Code, Message: te.Message})
	}
	if err := d.tasks.Save(context.Background(), *task); err != nil {
		d.hub.Finish(task.ID)
		return fmt.Errorf("save final task: %w", err)
	}
	d.hub.Finish(task.ID)
	return nil
}

// Cancel 取消运行中的任务（由 DELETE /tasks/:id 调用）。尽力传播：取消 ctx、
// 状态已由调用方迁移为 cancelled，Execute 循环里的 Transition 会被状态机拒绝。
func (d *Dispatcher) Cancel(taskID string) {
	d.mu.Lock()
	cancel, ok := d.cancels[taskID]
	d.mu.Unlock()
	if ok {
		cancel()
	}
}

func (d *Dispatcher) register(taskID string, cancel context.CancelFunc) {
	d.mu.Lock()
	d.cancels[taskID] = cancel
	d.mu.Unlock()
}

func (d *Dispatcher) unregister(taskID string) {
	d.mu.Lock()
	delete(d.cancels, taskID)
	d.mu.Unlock()
}

// 确保 RetryableError 实现 error（编译期自检）。
var _ error = (*RetryableError)(nil)
