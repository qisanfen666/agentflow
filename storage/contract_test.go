package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qisanfen666/agentflow/model"
)

// 本文件是存储层合同测试套件：同一组断言跑 memory / redis 两个实现。
// 接口语义（store.go 注释 + docs/contracts 合同文档）由测试守护——
// 任何新实现（SQL、Mongo 等）跑通同一套断言即视为符合合同。
// 必须先过这套测试。这是"接口即合同"的验收机制。

// storeFactory 每个测试拿到干净的存储实例。
type storeFactory func(t *testing.T) (AgentStore, TaskStore)

func validSpec() model.AgentSpec {
	return model.AgentSpec{
		Name:    "chat-agent",
		Type:    "chat",
		Runtime: model.RuntimeSpec{Type: model.RuntimePythonHTTP, Host: "http://localhost:8081"},
	}
}

func runAgentLifecycle(t *testing.T, mk storeFactory) {
	s, _ := mk(t)
	ctx := context.Background()

	// 创建：ID/version 由存储层分配
	spec, err := s.Create(ctx, validSpec())
	if err != nil {
		t.Fatal(err)
	}
	if spec.ID == "" || spec.Version != 1 {
		t.Fatalf("create should assign id and version=1, got %q v%d", spec.ID, spec.Version)
	}

	// 更新：新版本生效，历史版本不可变
	next := validSpec()
	next.Name = "chat-agent-v2"
	updated, err := s.Update(ctx, spec.ID, 1, next)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 {
		t.Fatalf("update should create version=2, got %d", updated.Version)
	}
	old, err := s.GetVersion(ctx, spec.ID, 1)
	if err != nil || old.Name != "chat-agent" {
		t.Fatalf("old version must stay immutable, got %+v err=%v", old, err)
	}

	// 版本历史升序且完整
	vs, err := s.Versions(ctx, spec.ID)
	if err != nil || len(vs) != 2 || vs[0].Version != 1 || vs[1].Version != 2 {
		t.Fatalf("versions should be [1,2], got %+v err=%v", vs, err)
	}

	// 软删除：Get 拒绝，GetVersion 仍可读（存量任务继续执行）
	if err := s.SoftDelete(ctx, spec.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, spec.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after soft-delete should be ErrNotFound, got %v", err)
	}
	if _, err := s.GetVersion(ctx, spec.ID, 2); err != nil {
		t.Fatalf("getversion after soft-delete must still work, got %v", err)
	}

	// 已删除不能更新
	if _, err := s.Update(ctx, spec.ID, 2, validSpec()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update after soft-delete should be ErrNotFound, got %v", err)
	}
}

func runOptimisticLock(t *testing.T, mk storeFactory) {
	s, _ := mk(t)
	ctx := context.Background()
	spec, _ := s.Create(ctx, validSpec())
	_, _ = s.Update(ctx, spec.ID, 1, validSpec()) // 现在 latest = v2

	// 拿过期的 base=1 再更新：必须冲突
	_, err := s.Update(ctx, spec.ID, 1, validSpec())
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale baseVersion should conflict, got %v", err)
	}
}

// runConcurrentUpdateAtomicity 并发 20 个写者都从 base=1 更新：
// 恰好一个成功，其余 ErrVersionConflict；最终版本 [1,2] 无空洞无重复。
// 这是版本原子递增的直接验证（memory 靠互斥锁 / redis 靠 Lua，殊途同归）。
func runConcurrentUpdateAtomicity(t *testing.T, mk storeFactory) {
	s, _ := mk(t)
	ctx := context.Background()
	spec, _ := s.Create(ctx, validSpec())

	const n = 20
	var conflicts int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			next := validSpec()
			next.Name = "racer"
			if _, err := s.Update(ctx, spec.ID, 1, next); errors.Is(err, ErrVersionConflict) {
				atomic.AddInt32(&conflicts, 1)
			}
		}()
	}
	wg.Wait()

	if conflicts != n-1 {
		t.Fatalf("exactly 1 of %d should win, conflicts = %d", n, conflicts)
	}
	vs, _ := s.Versions(ctx, spec.ID)
	if len(vs) != 2 {
		t.Fatalf("versions should be [1,2], got %d entries", len(vs))
	}
}

func runTaskBasics(t *testing.T, mk storeFactory) {
	_, s := mk(t)
	ctx := context.Background()

	// 非 pending 的任务不允许 Create
	running := model.Task{Status: model.TaskRunning}
	if _, err := s.Create(ctx, running); err == nil {
		t.Fatal("create must reject non-pending task")
	}

	created, err := s.Create(ctx, model.Task{Status: model.TaskPending, AgentID: "a_x"})
	if err != nil || created.ID == "" {
		t.Fatalf("create should assign id, err=%v", err)
	}

	// 状态迁移后 Save 生效
	if err := created.Transition(model.TaskRunning); err != nil {
		t.Fatal(err)
	}
	if err := created.Transition(model.TaskSucceeded); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, created); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, created.ID)
	if got.Status != model.TaskSucceeded || got.AttemptCount != 1 {
		t.Fatalf("saved task should be succeeded/attempt=1, got %s/%d", got.Status, got.AttemptCount)
	}

	// Save 未知 ID 拒绝
	if err := s.Save(ctx, model.Task{ID: "t_missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("save unknown id should be ErrNotFound, got %v", err)
	}
}

// runTaskFinalityGuard 终态不可逆：已终态的任务拒绝被覆盖为其他状态，
// 同终态重写放行。守护"取消 vs 执行收尾"竞态——并发写者各持分叉的本地副本，
// 后落库者不得复活已终态的任务（否则被取消的任务会重试重跑）。
func runTaskFinalityGuard(t *testing.T, mk storeFactory) {
	_, s := mk(t)
	ctx := context.Background()

	created, err := s.Create(ctx, model.Task{Status: model.TaskPending, AgentID: "a_x"})
	if err != nil {
		t.Fatal(err)
	}

	// 复刻竞态：两个写者从同一 running 副本分叉
	runner := created // 执行侧副本（dispatcher）
	if err := runner.Transition(model.TaskRunning); err != nil {
		t.Fatal(err)
	}
	cancelled := runner // 取消侧副本（handler Get 到的正是 running）
	if err := cancelled.Transition(model.TaskCancelled); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, cancelled); err != nil {
		t.Fatal(err)
	}

	// 执行侧本地仍是 running：回退 pending（状态机合法，为崩溃自愈设计），
	// 但落库必须被存储层拒绝
	if err := runner.Transition(model.TaskPending); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, runner); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("overwriting final task must conflict, got %v", err)
	}

	// 同终态重写放行（幂等）
	if err := s.Save(ctx, cancelled); err != nil {
		t.Fatalf("re-saving same final state must pass, got %v", err)
	}

	got, _ := s.Get(ctx, created.ID)
	if got.Status != model.TaskCancelled {
		t.Fatalf("cancelled task must stay cancelled, got %s", got.Status)
	}
}

// idemFactory 每个测试拿到干净的幂等存储。
type idemFactory func(t *testing.T) IdemStore

func runIdemSemantics(t *testing.T, mk idemFactory) {
	s := mk(t)
	ctx := context.Background()
	key := "k_" + newID("t")

	// 首次占位成功
	existing, inserted, err := s.PutIfAbsent(ctx, key, "t_1", time.Minute)
	if err != nil || !inserted || existing != "" {
		t.Fatalf("first put should insert, got existing=%q inserted=%v err=%v", existing, inserted, err)
	}
	// 重复提交返回原任务
	existing, inserted, err = s.PutIfAbsent(ctx, key, "t_2", time.Minute)
	if err != nil || inserted || existing != "t_1" {
		t.Fatalf("second put should return t_1, got existing=%q inserted=%v err=%v", existing, inserted, err)
	}
	// TTL 过期后可重新占位
	if _, _, err := s.PutIfAbsent(ctx, key+"_ttl", "t_1", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	existing, inserted, err = s.PutIfAbsent(ctx, key+"_ttl", "t_3", time.Minute)
	if err != nil || !inserted || existing != "" {
		t.Fatalf("expired key should re-insert, got existing=%q inserted=%v err=%v", existing, inserted, err)
	}
}

// toolFactory 每个测试拿到干净的注册表。
type toolFactory func(t *testing.T) ToolStore

func validTool() model.ToolDef {
	return model.ToolDef{
		Name:        "search_docs",
		Description: "在知识库中检索文档",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string"},
			},
			"required": []any{"query"},
		},
		TimeoutSec: 30,
	}
}

func runToolSemantics(t *testing.T, mk toolFactory) {
	s := mk(t)
	ctx := context.Background()

	// 注册：ID/CreatedAt 由存储层填充
	tool, err := s.Create(ctx, validTool())
	if err != nil {
		t.Fatal(err)
	}
	if tool.ID == "" || tool.CreatedAt.IsZero() {
		t.Fatalf("store must fill ID/CreatedAt, got %+v", tool)
	}

	// 名字唯一
	if _, err := s.Create(ctx, validTool()); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate name should fail with ErrDuplicate, got %v", err)
	}

	// Get 往返：嵌套 parameters（map/slice）无损
	got, err := s.Get(ctx, tool.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != tool.Name || got.TimeoutSec != 30 {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
	props, _ := got.Parameters["properties"].(map[string]any)
	if len(props) != 1 {
		t.Fatalf("parameters lost in roundtrip: %+v", got.Parameters)
	}

	// GetByName（MCP 导出入口）
	byName, err := s.GetByName(ctx, tool.Name)
	if err != nil || byName.ID != tool.ID {
		t.Fatalf("GetByName(%s) = %+v err=%v", tool.Name, byName, err)
	}

	// 不存在
	if _, err := s.Get(ctx, "tool_none"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if _, err := s.GetByName(ctx, "no_such_tool"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	// List 全量
	list, err := s.List(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("List = %d items err=%v, want 1", len(list), err)
	}
}
