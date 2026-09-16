package engine

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/qisanfen666/agentflow/model"
)

// Redis 队列集成测试：与 storage 包同一套约定——AGENTFLOW_TEST_REDIS 地址
// （默认 6379）。DB 隔离到 2（storage 契约测试占 1）：go test 按包并行跑
// 两个测试二进制，同 DB 会互相 FlushDB 干扰。
// 不可达则 skip。

func redisQueue(t *testing.T) (*RedisQueue, *redis.Client) {
	t.Helper()
	addr := os.Getenv("AGENTFLOW_TEST_REDIS")
	if addr == "" {
		addr = "localhost:6379"
	}
	db, _ := strconv.Atoi(os.Getenv("AGENTFLOW_TEST_REDIS_DB"))
	if db == 0 {
		db = 2
	}
	c := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		t.Skipf("redis 不可达(%v)，跳过集成测试", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	return NewRedisQueue(c, 0), c
}

// mustDequeue 带时限取任务，超时即测试失败（防阻塞型实现把测试挂死）。
func mustDequeue(t *testing.T, q *RedisQueue) model.Task {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	task, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	return task
}

func mkTask(id string) model.Task {
	return model.Task{ID: id, Status: model.TaskPending, AgentID: "a_1", AgentVersion: 1}
}

func TestRedisQueueFIFO(t *testing.T) {
	q, c := redisQueue(t)
	ctx := context.Background()

	for _, id := range []string{"t_1", "t_2", "t_3"} {
		if err := q.Enqueue(ctx, mkTask(id)); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"t_1", "t_2", "t_3"} {
		if got := mustDequeue(t, q); got.ID != want {
			t.Fatalf("dequeue order broken: got %s, want %s", got.ID, want)
		}
	}
	if n, _ := c.LLen(ctx, keyProcessing).Result(); n != 3 {
		t.Fatalf("processing len = %d, want 3", n)
	}
}

func TestRedisQueueAckRemovesEverything(t *testing.T) {
	q, c := redisQueue(t)
	ctx := context.Background()

	_ = q.Enqueue(ctx, mkTask("t_a"))
	task := mustDequeue(t, q)
	if err := q.Ack(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.LLen(ctx, keyProcessing).Result(); n != 0 {
		t.Fatalf("processing len = %d, want 0", n)
	}
	if n, _ := c.HLen(ctx, keyTasks).Result(); n != 0 {
		t.Fatalf("tasks hash len = %d, want 0（正身必须被清）", n)
	}
}

// TestRedisQueueGhostEntry 幽灵条目：ID 在 ready 但正身已删（Ack 后的重复投递），
// Dequeue 必须跳过它并清理 processing，随后取到正常任务。
func TestRedisQueueGhostEntry(t *testing.T) {
	q, c := redisQueue(t)
	ctx := context.Background()

	_ = c.LPush(ctx, keyReady, "t_ghost") // 无正身
	_ = q.Enqueue(ctx, mkTask("t_real"))

	if got := mustDequeue(t, q); got.ID != "t_real" {
		t.Fatalf("ghost not skipped, got %s", got.ID)
	}
	if n, _ := c.LLen(ctx, keyProcessing).Result(); n != 1 {
		t.Fatalf("processing len = %d, want 1（ghost 应被清理，real 在途）", n)
	}
}

// TestRedisQueueNackImmediate 立即退回 + 新鲜副本生效：
// Nack 传入的 task 是回读过存储的（attempt 已推进），队列必须以它为准。
func TestRedisQueueNackImmediate(t *testing.T) {
	q, _ := redisQueue(t)
	ctx := context.Background()

	_ = q.Enqueue(ctx, mkTask("t_n"))
	_ = mustDequeue(t, q)

	fresh := mkTask("t_n")
	fresh.AttemptCount = 2 // 模拟 worker 回读后的新副本
	if err := q.Nack(ctx, fresh, 0); err != nil {
		t.Fatal(err)
	}
	got := mustDequeue(t, q)
	if got.AttemptCount != 2 {
		t.Fatalf("requeued task attempt = %d, want 2（必须携带最新副本）", got.AttemptCount)
	}
}

// TestRedisQueueNackDelayed 延迟退回：nack(delay) 后立即取不到，
// mover（Start 启动）到期后放行。
func TestRedisQueueNackDelayed(t *testing.T) {
	q, c := redisQueue(t)
	ctx := context.Background()
	stop := q.Start(ctx)
	t.Cleanup(stop)

	_ = q.Enqueue(ctx, mkTask("t_d"))
	_ = mustDequeue(t, q)
	if err := q.Nack(ctx, mkTask("t_d"), 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	dctx, dcancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer dcancel()
	if _, err := q.Dequeue(dctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delayed task must not be visible before deadline, err=%v", err)
	}
	// 到期后（mover 200ms 粒度 + 300ms 延迟）应可取回
	bctx, bcancel := context.WithTimeout(ctx, 3*time.Second)
	defer bcancel()
	task, err := q.Dequeue(bctx)
	if err != nil {
		t.Fatalf("delayed task should be back: %v", err)
	}
	if task.ID != "t_d" {
		t.Fatalf("got %s, want t_d", task.ID)
	}
	_ = c
}

// TestRedisQueueReclaimCrashedWorker 模拟 worker 崩溃：
// 取出后不 Ack，手动删租约（= 可见性超时已过），Reclaim 应推回 ready。
func TestRedisQueueReclaimCrashedWorker(t *testing.T) {
	q, c := redisQueue(t)
	ctx := context.Background()

	_ = q.Enqueue(ctx, mkTask("t_c"))
	task := mustDequeue(t, q)

	// 模拟租约过期（不等待真实 TTL）
	_ = c.Del(ctx, "agentflow:q:seen:"+task.ID)

	n, err := q.Reclaim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reclaimed = %d, want 1", n)
	}
	if got := mustDequeue(t, q); got.ID != "t_c" {
		t.Fatalf("requeued task = %s, want t_c", got.ID)
	}
}

// TestRedisQueuePoisonDropped 毒丸：正身是坏 JSON 时 Dequeue 丢弃并继续，
// 不阻塞后续任务。
func TestRedisQueuePoisonDropped(t *testing.T) {
	q, c := redisQueue(t)
	ctx := context.Background()

	_ = c.HSet(ctx, keyTasks, "t_p", "{not-json")
	_ = c.LPush(ctx, keyReady, "t_p")
	_ = q.Enqueue(ctx, mkTask("t_ok"))

	if got := mustDequeue(t, q); got.ID != "t_ok" {
		t.Fatalf("poison blocked the queue, got %s", got.ID)
	}
}
