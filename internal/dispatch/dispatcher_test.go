package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/qisanfen666/agentflow/internal/observability"
	"github.com/qisanfen666/agentflow/model"
	"github.com/qisanfen666/agentflow/runtime"
	"github.com/qisanfen666/agentflow/storage"
)

// fakeRuntime 按脚本回放事件的假 Runtime。
// Type 返回 python-http：model.Validate 只认内置类型，fake 冒名顶替以通过校验
// （自定义类型注册机制属于 facade 的配置项）。
type fakeRuntime struct {
	events     []runtime.Event
	blockOnCtx bool // true：挂起直到 ctx 取消，然后直接关通道（不给终止事件）
}

func (f *fakeRuntime) Type() string                                        { return model.RuntimePythonHTTP }
func (f *fakeRuntime) Health(_ context.Context, _ model.RuntimeSpec) error { return nil }

func (f *fakeRuntime) Execute(ctx context.Context, _ runtime.Request) <-chan runtime.Event {
	ch := make(chan runtime.Event, 8)
	go func() {
		defer close(ch)
		if f.blockOnCtx {
			<-ctx.Done()
			return
		}
		for _, ev := range f.events {
			ch <- ev
		}
	}()
	return ch
}

func setup(t *testing.T, rt runtime.Runtime) (*Dispatcher, storage.TaskStore, *Hub, model.Task) {
	return setupWithAudit(t, rt, nil)
}

func setupWithAudit(t *testing.T, rt runtime.Runtime, audit observability.AuditLogger) (*Dispatcher, storage.TaskStore, *Hub, model.Task) {
	t.Helper()
	ctx := context.Background()
	agents := storage.NewMemoryAgentStore()
	tasks := storage.NewMemoryTaskStore()
	hub := NewHub()
	d := New(agents, tasks, hub, observability.Telemetry{Audit: audit}, rt)

	spec, err := agents.Create(ctx, model.AgentSpec{
		Name:    "fake-agent",
		Type:    "chat",
		Runtime: model.RuntimeSpec{Type: model.RuntimePythonHTTP, Host: "http://fake"},
	})
	if err != nil {
		t.Fatal(err)
	}
	task, err := tasks.Create(ctx, model.Task{
		Status:       model.TaskPending,
		AgentID:      spec.ID,
		AgentVersion: spec.Version,
		Payload:      map[string]any{"q": "hi"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d, tasks, hub, task
}

// waitFinal 轮询直到任务进入终态（上限 2s）。
func waitFinal(t *testing.T, tasks storage.TaskStore, id string) model.Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := tasks.Get(context.Background(), id)
		if err == nil && task.Status.Final() {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	task, _ := tasks.Get(context.Background(), id)
	t.Fatalf("task %s not final, status=%s", id, task.Status)
	return model.Task{}
}

// waitHubFinished 轮询直到事件流结束（上限 2s）。
func waitHubFinished(t *testing.T, hub *Hub, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, fin, ok := hub.Since(id, 0); ok && fin {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("hub stream for %s not finished", id)
}

func TestSuccessPath(t *testing.T) {
	rt := &fakeRuntime{events: []runtime.Event{
		{Type: runtime.EventToken, Content: "Hel"},
		{Type: runtime.EventToken, Content: "lo"},
		{Type: runtime.EventUsage, PromptTokens: 10, CompletionTokens: 5, Model: "m1"},
		{Type: runtime.EventUsage, PromptTokens: 7, CompletionTokens: 3}, // 第二次 usage 应累加
		{Type: runtime.EventDone, TaskID: "t"},
	}}
	d, tasks, hub, task := setup(t, rt)

	go func() { _ = d.Execute(task) }()
	final := waitFinal(t, tasks, task.ID)

	if final.Status != model.TaskSucceeded {
		t.Fatalf("status = %s, want succeeded", final.Status)
	}
	if final.Usage == nil || final.Usage.PromptTokens != 17 || final.Usage.CompletionTokens != 8 {
		t.Fatalf("usage should accumulate 17/8, got %+v", final.Usage)
	}
	if final.Usage.Model != "m1" {
		t.Fatalf("model should be kept, got %q", final.Usage.Model)
	}
	waitHubFinished(t, hub, task.ID)
}

func TestFailedPath(t *testing.T) {
	rt := &fakeRuntime{events: []runtime.Event{
		{Type: runtime.EventToken, Content: "partial"},
		{Type: runtime.EventError, Code: model.ErrToolTimeout, Message: "boom"},
	}}
	d, tasks, hub, task := setup(t, rt)

	go func() { _ = d.Execute(task) }()
	final := waitFinal(t, tasks, task.ID)

	if final.Status != model.TaskFailed {
		t.Fatalf("status = %s, want failed", final.Status)
	}
	if final.Error == nil || final.Error.Code != model.ErrToolTimeout {
		t.Fatalf("error code lost, got %+v", final.Error)
	}
	waitHubFinished(t, hub, task.ID)
}

// 错误分类学决策：AGENT_TIMEOUT 可重试——先回退 pending 交给 worker；
// 重试名额耗尽后才落 timeout 终态。两个路径分别验证。
func TestTimeoutExhaustedToFinal(t *testing.T) {
	rt := &fakeRuntime{events: []runtime.Event{
		{Type: runtime.EventError, Code: model.ErrAgentTimeout, Message: "deadline"},
	}}
	d, tasks, _, task := setup(t, rt)
	d.MaxRetries = 0 // 名额耗尽：直接终态

	go func() { _ = d.Execute(task) }()
	final := waitFinal(t, tasks, task.ID)

	if final.Status != model.TaskTimeout {
		t.Fatalf("status = %s, want timeout", final.Status)
	}
}

func TestRetryableErrorBackToPending(t *testing.T) {
	rt := &fakeRuntime{events: []runtime.Event{
		{Type: runtime.EventError, Code: model.ErrAgentTimeout, Message: "deadline"},
	}}
	d, tasks, hub, task := setup(t, rt) // 默认 MaxRetries=3，还有名额

	err := d.Execute(task)

	var re *RetryableError
	if !errors.As(err, &re) || re.Code != model.ErrAgentTimeout {
		t.Fatalf("Execute should return RetryableError, got %v", err)
	}
	got, _ := tasks.Get(context.Background(), task.ID)
	if got.Status != model.TaskPending {
		t.Fatalf("task should be back to pending for retry, got %s", got.Status)
	}
	if _, finished, _ := hub.Since(task.ID, 0); finished {
		t.Fatal("hub must stay open across retries")
	}
}

// TestCancelRunning 取消运行中的任务：
// 模拟 API handler 行为（迁 cancelled + Save + Cancel），fake 挂起等待 ctx，
// 通道关闭无终止事件——终态 cancelled 必须保持不变，且 Hub 最终关闭。
func TestCancelRunning(t *testing.T) {
	rt := &fakeRuntime{blockOnCtx: true}
	d, tasks, hub, task := setup(t, rt)
	ctx := context.Background()

	go func() { _ = d.Execute(task) }()

	// 等到 running 再取消，避免与 pending->running 竞态
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cur, _ := tasks.Get(ctx, task.ID)
		if cur.Status == model.TaskRunning {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cur, err := tasks.Get(ctx, task.ID)
	if err != nil || cur.Status != model.TaskRunning {
		t.Fatalf("task should reach running, got %+v err=%v", cur, err)
	}

	// API handler 的取消序列
	if err := cur.Transition(model.TaskCancelled); err != nil {
		t.Fatal(err)
	}
	if err := tasks.Save(ctx, cur); err != nil {
		t.Fatal(err)
	}
	d.Cancel(cur.ID)

	final := waitFinal(t, tasks, task.ID)
	if final.Status != model.TaskCancelled {
		t.Fatalf("cancelled must win, got %s", final.Status)
	}
	waitHubFinished(t, hub, task.ID)
}

func TestHubBasics(t *testing.T) {
	h := NewHub()

	if _, _, ok := h.Since("missing", 0); ok {
		t.Fatal("unknown task should report ok=false")
	}

	h.Publish("t1", runtime.Event{Type: runtime.EventToken, Content: "a"})
	h.Publish("t1", runtime.Event{Type: runtime.EventToken, Content: "b"})

	evs, fin, ok := h.Since("t1", 0)
	if !ok || fin || len(evs) != 2 {
		t.Fatalf("want 2 live events, got %d ok=%v fin=%v", len(evs), ok, fin)
	}
	evs, _, _ = h.Since("t1", 1)
	if len(evs) != 1 || evs[0].Content != "b" {
		t.Fatalf("Since(1) should tail from index 1, got %+v", evs)
	}

	h.Finish("t1")
	_, fin, _ = h.Since("t1", 0)
	if !fin {
		t.Fatal("finished flag should be set")
	}
	select {
	case <-h.Notify("t1"):
		// close 后立即返回：订阅循环可自然终止
	default:
		t.Fatal("notify must be readable after finish")
	}
}

// ---------- 审计埋点 ----------

// bufAudit 测试用审计收集器。
type bufAudit struct {
	mu  sync.Mutex
	evs []observability.AuditEvent
}

func (b *bufAudit) Record(_ context.Context, ev observability.AuditEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.evs = append(b.evs, ev)
	return nil
}

func (b *bufAudit) actions() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.evs))
	for i, ev := range b.evs {
		out[i] = ev.Action
	}
	return out
}

func (b *bufAudit) last() observability.AuditEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.evs[len(b.evs)-1]
}

func TestAuditTrailOnSuccess(t *testing.T) {
	rt := &fakeRuntime{events: []runtime.Event{
		{Type: runtime.EventToken, Content: "hi"},
		{Type: runtime.EventUsage, PromptTokens: 17, CompletionTokens: 8, Model: "m1"},
		{Type: runtime.EventDone, TaskID: "t"},
	}}
	audit := &bufAudit{}
	d, tasks, hub, task := setupWithAudit(t, rt, audit)

	go func() { _ = d.Execute(task) }()
	waitFinal(t, tasks, task.ID)
	waitHubFinished(t, hub, task.ID)

	got := audit.actions()
	want := []string{observability.ActionTaskStarted, observability.ActionTaskSucceeded}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("audit trail = %v, want %v", got, want)
	}
	final := audit.last()
	if final.Entity != observability.EntityTask || final.EntityID != task.ID {
		t.Fatalf("event entity mismatch: %+v", final)
	}
	if final.Detail["prompt_tokens"] != int64(17) || final.Detail["model"] != "m1" {
		t.Fatalf("succeeded audit should carry usage summary, got %+v", final.Detail)
	}
}

func TestAuditTrailOnFailure(t *testing.T) {
	rt := &fakeRuntime{events: []runtime.Event{
		{Type: runtime.EventError, Code: model.ErrToolTimeout, Message: "boom"},
	}}
	audit := &bufAudit{}
	d, tasks, hub, task := setupWithAudit(t, rt, audit)

	go func() { _ = d.Execute(task) }()
	waitFinal(t, tasks, task.ID)
	waitHubFinished(t, hub, task.ID)

	got := audit.actions()
	want := []string{observability.ActionTaskStarted, observability.ActionTaskFailed}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("audit trail = %v, want %v", got, want)
	}
	if last := audit.last(); last.Detail["error_code"] != model.ErrToolTimeout {
		t.Fatalf("failed audit should carry error_code, got %+v", last.Detail)
	}
}

func TestAuditTrailOnRetry(t *testing.T) {
	rt := &fakeRuntime{events: []runtime.Event{
		{Type: runtime.EventError, Code: model.ErrAgentTimeout, Message: "deadline"},
	}}
	audit := &bufAudit{}
	d, _, _, task := setupWithAudit(t, rt, audit)

	if err := d.Execute(task); err == nil {
		t.Fatal("should return RetryableError")
	}
	if got := audit.actions(); len(got) != 2 || got[1] != observability.ActionTaskRetry {
		t.Fatalf("audit trail = %v, want [task_started task_retry]", got)
	}
	if last := audit.last(); last.Detail["error_code"] != model.ErrAgentTimeout || last.Detail["attempt"] != 1 {
		t.Fatalf("retry audit should carry attempt+error_code, got %+v", last.Detail)
	}
}

// TestTraceLinkOnExecute 跨队列因果：任务携带提交侧 traceparent，
// task.execute span 以 Link 指回该 SpanContext（traceID 一致）。
func TestTraceLinkOnExecute(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	const (
		traceID = "992f7b14ac8122581e4c672c6423c5c5"
		spanID  = "b9813c58a01f1234"
	)
	taskCtx := "00-" + traceID + "-" + spanID + "-01"

	rt := &fakeRuntime{events: []runtime.Event{{Type: runtime.EventDone, TaskID: "t"}}}
	d, tasks, hub, task := setupWithAudit(t, rt, nil)
	task.TraceContext = taskCtx
	if err := d.Execute(task); err != nil {
		t.Fatal(err)
	}
	waitFinal(t, tasks, task.ID)
	waitHubFinished(t, hub, task.ID)

	for _, span := range rec.Ended() {
		if span.Name() != "task.execute" {
			continue
		}
		links := span.Links()
		if len(links) == 0 {
			t.Fatal("task.execute should carry a link to the submit-side span")
		}
		if got := links[0].SpanContext.TraceID().String(); got != traceID {
			t.Fatalf("link trace id = %s, want %s", got, traceID)
		}
		// Link 不是父子：task.execute 自身是独立 trace 的根
		if span.Parent().IsValid() {
			t.Fatal("task.execute must stay a root span (Link, not CHILD_OF)")
		}
		return
	}
	t.Fatal("task.execute span not recorded")
}
