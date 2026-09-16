package storage

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis 集成测试：依赖真实实例（用户约定用容器提供）。
// 地址取 AGENTFLOW_TEST_REDIS（默认 localhost:6379）；DB 取 AGENTFLOW_TEST_REDIS_DB
// （默认 1——隔离到专用 DB，避免误清共享实例 DB 0 的其他项目数据）；
// 不可达则 skip，保证"本地单测零外部依赖"的约定不被破坏。
//
// 启动测试实例: docker run -d --rm --name agentflow-test-redis -p 6379:6379 redis:7-alpine

func redisAddr() string {
	if a := os.Getenv("AGENTFLOW_TEST_REDIS"); a != "" {
		return a
	}
	return "localhost:6379"
}

func redisDB() int {
	n, _ := strconv.Atoi(os.Getenv("AGENTFLOW_TEST_REDIS_DB"))
	if n == 0 {
		n = 1
	}
	return n
}

// redisTestClient 返回已连通、已清库（当前 DB）的客户端；不可达时 skip 当前测试。
func redisTestClient(t *testing.T) redis.UniversalClient {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: redisAddr(), DB: redisDB()})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("redis 不可达(%v)，跳过集成测试", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	return client
}

func redisFactory(t *testing.T) (AgentStore, TaskStore) {
	t.Helper()
	c := redisTestClient(t)
	return NewRedisAgentStore(c), NewRedisTaskStore(c)
}

func TestRedisAgentLifecycle(t *testing.T) { runAgentLifecycle(t, redisFactory) }

func TestRedisOptimisticLock(t *testing.T) { runOptimisticLock(t, redisFactory) }

func TestRedisConcurrentUpdateAtomicity(t *testing.T) { runConcurrentUpdateAtomicity(t, redisFactory) }

func TestRedisTaskBasics(t *testing.T) { runTaskBasics(t, redisFactory) }

func TestRedisIdem(t *testing.T) {
	c := redisTestClient(t)
	runIdemSemantics(t, func(t *testing.T) IdemStore { return NewRedisIdemStore(c) })
}

func TestRedisTools(t *testing.T) {
	c := redisTestClient(t)
	runToolSemantics(t, func(t *testing.T) ToolStore { return NewRedisToolStore(c) })
}

// TestRedisPersistenceAcrossInstances 持久化的存在意义：
// 换一个全新客户端（模拟进程重启后重新连接），数据仍然在。
func TestRedisPersistenceAcrossInstances(t *testing.T) {
	c := redisTestClient(t)
	ctx := context.Background()

	a := NewRedisAgentStore(c)
	spec, err := a.Create(ctx, validSpec())
	if err != nil {
		t.Fatal(err)
	}

	// 全新客户端 = 模拟 server 重启后的世界（同一个隔离 DB）
	bc := redis.NewClient(&redis.Options{Addr: redisAddr(), DB: redisDB()})
	t.Cleanup(func() { _ = bc.Close() })
	b := NewRedisAgentStore(bc)
	got, err := b.Get(ctx, spec.ID)
	if err != nil {
		t.Fatalf("data must survive across clients: %v", err)
	}
	if got.ID != spec.ID || got.Version != 1 {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
}
