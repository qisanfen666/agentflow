package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qisanfen666/agentflow/internal/dispatch"
	"github.com/qisanfen666/agentflow/model"
	"github.com/qisanfen666/agentflow/runtime"
	"github.com/qisanfen666/agentflow/storage"
)

// flakyRuntime 前 failN 次调用返回可重试错误，之后成功。
type flakyRuntime struct {
	failN int
	calls atomic.Int32
}

func (f *flakyRuntime) Type() string                                        { return model.RuntimePythonHTTP }
func (f *flakyRuntime) Health(_ context.Context, _ model.RuntimeSpec) error { return nil }

func (f *flakyRuntime) Execute(_ context.Context, _ runtime.Request) <-chan runtime.Event {
	ch := make(chan runtime.Event, 4)
	go func() {
		defer close(ch)
		if f.calls.Add(1) <= int32(f.failN) {
			ch <- runtime.Event{Type: runtime.EventError, Code: model.ErrAgentUnreachable, Message: "flaky"}
			return
		}
		ch <- runtime.Event{Type: runtime.EventToken, Content: "ok"}
		ch <- runtime.Event{Type: runtime.EventDone}
	}()
	return ch
}

// failAlwaysRuntime 永远失败，错误码可配置（验证不可重试直落终态）。
type failAlwaysRuntime struct{ code string }

func (f *failAlwaysRuntime) Type() string                                        { return model.RuntimePythonHTTP }
func (f *failAlwaysRuntime) Health(_ context.Context, _ model.RuntimeSpec) error { return nil }

func (f *failAlwaysRuntime) Execute(_ context.Context, _ runtime.Request) <-chan runtime.Event {
	ch := make(chan runtime.Event, 1)
	go func() {
		defer close(ch)
		ch <- runtime.Event{Type: runtime.EventError, Code: f.code, Message: "permanent"}
	}()
	return ch
}

// setupEngine 装配完整执行引擎（memory 全家桶 + 快退避）。
func setupEngine(t *testing.T, rt runtime.Runtime, cfg WorkerConfig) (engineDeps, model.Task) {
	t.Helper()
	ctx := context.Background()
	agents := storage.NewMemoryAgentStore()
	tasks := storage.NewMemoryTaskStore()
	hub := dispatch.NewHub()
	d := dispatch.New(agents, tasks, hub, rt)
	q := NewMemoryQueue()
	w := NewWorker(q, d, tasks, cfg)
	stop := w.Start(ctx)
	t.Cleanup(stop)

	spec, err := agents.Create(ctx, model.AgentSpec{
		Name: "e2e", Type: "chat",
		Runtime: model.RuntimeSpec{Type: model.RuntimePythonHTTP, Host: "http://fake"},
	})
	if err != nil {
		t.Fatal(err)
	}
	task, err := tasks.Create(ctx, model.Task{
		Status: model.TaskPending, AgentID: spec.ID, AgentVersion: spec.Version,
	})
	if err != nil {
		t.Fatal(err)
	}
	return engineDeps{tasks: tasks, queue: q}, task
}

type engineDeps struct {
	tasks storage.TaskStore
	queue Queue
}

func (e engineDeps) waitFinal(t *testing.T, id string) model.Task {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		task, err := e.tasks.Get(context.Background(), id)
		if err == nil && task.Status.Final() {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	task, _ := e.tasks.Get(context.Background(), id)
	t.Fatalf("task %s not final, status=%s", id, task.Status)
	return model.Task{}
}

// 快退避配置（10ms 基数，重试链路毫秒级完成）。
func fastCfg(maxRetries int) WorkerConfig {
	return WorkerConfig{MaxRetries: maxRetries, RetryBackoffBase: 10 * time.Millisecond}
}

// TestRetryUntilSuccess 失败 2 次后第 3 次成功：
// 验证 退避重投 -> attempt 推进 -> 最终 succeeded。
func TestRetryUntilSuccess(t *testing.T) {
	deps, task := setupEngine(t, &flakyRuntime{failN: 2}, fastCfg(3))

	if err := deps.queue.Enqueue(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	final := deps.waitFinal(t, task.ID)

	if final.Status != model.TaskSucceeded {
		t.Fatalf("status = %s, want succeeded", final.Status)
	}
	if final.AttemptCount != 3 {
		t.Fatalf("attempt = %d, want 3 (attempt 必须推进，否则无限重试)", final.AttemptCount)
	}
}

// TestRetryExhaustedToFailed 永远失败 + MaxRetries=1：
// 首跑 1 次 + 重试 1 次 = attempt 2，然后落 failed。
func TestRetryExhaustedToFailed(t *testing.T) {
	deps, task := setupEngine(t, &flakyRuntime{failN: 99}, fastCfg(1))

	if err := deps.queue.Enqueue(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	final := deps.waitFinal(t, task.ID)

	if final.Status != model.TaskFailed {
		t.Fatalf("status = %s, want failed", final.Status)
	}
	if final.AttemptCount != 2 {
		t.Fatalf("attempt = %d, want 2 (首跑+1次重试)", final.AttemptCount)
	}
	if final.Error == nil || final.Error.Code != model.ErrAgentUnreachable {
		t.Fatalf("error code lost: %+v", final.Error)
	}
}

// TestNotRetryableFailsFast 不可重试错误（TOOL_TIMEOUT）不重试，直落 failed。
func TestNotRetryableFailsFast(t *testing.T) {
	deps, task := setupEngine(t, &failAlwaysRuntime{code: model.ErrToolTimeout}, fastCfg(3))

	if err := deps.queue.Enqueue(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	final := deps.waitFinal(t, task.ID)

	if final.Status != model.TaskFailed {
		t.Fatalf("status = %s, want failed", final.Status)
	}
	if final.AttemptCount != 1 {
		t.Fatalf("attempt = %d, want 1 (不可重试)", final.AttemptCount)
	}
}

// TestCrashedRunningTaskNormalized 模拟消费者崩溃后被重投的任务（status=running）：
// worker 归一化 running->pending 后正常执行到终态。
func TestCrashedRunningTaskNormalized(t *testing.T) {
	rt := &flakyRuntime{failN: 0}
	deps, task := setupEngine(t, rt, fastCfg(3))

	// 人为把任务做成"崩溃现场"：running 状态直接入队（可见性超时重投的形态）
	if err := task.Transition(model.TaskRunning); err != nil {
		t.Fatal(err)
	}
	if err := deps.tasks.Save(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if err := deps.queue.Enqueue(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	final := deps.waitFinal(t, task.ID)
	if final.Status != model.TaskSucceeded {
		t.Fatalf("status = %s, want succeeded (running 须被归一化)", final.Status)
	}
}

func TestBackoffCurve(t *testing.T) {
	w := &Worker{cfg: fastCfg(3)} // 不启动，只测退避计算
	base := 10 * time.Millisecond
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, base},
		{2, 4 * base},
		{3, 9 * base},
		{0, base}, // 越界钳到 1
	}
	for _, c := range cases {
		if got := w.backoff(c.attempt); got != c.want {
			t.Errorf("backoff(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}
