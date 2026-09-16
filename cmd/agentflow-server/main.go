// agentflow-server 是开箱即用的演示二进制：手工 DI 装配的最小示范。
// 库形态由根包 facade（agentflow.New）提供，优雅关停同属 facade 职责。
//
// 环境变量（facade 之外的简易配置面）：
//
//	AGENTFLOW_MODE           memory | redis（默认 memory）
//	AGENTFLOW_ADDR           监听地址（默认 :8080）
//	AGENTFLOW_REDIS_ADDR     Redis 地址（默认 localhost:6380，用 redis:7 容器）
//	AGENTFLOW_REDIS_DB       Redis DB（默认 0，键有 agentflow: 前缀不会撞库）
//	AGENTFLOW_VISIBILITY_SEC 在途租约/可见性超时秒数（默认 300）
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/qisanfen666/agentflow/internal/api"
	"github.com/qisanfen666/agentflow/internal/dispatch"
	"github.com/qisanfen666/agentflow/internal/engine"
	"github.com/qisanfen666/agentflow/internal/registry"
	"github.com/qisanfen666/agentflow/runtime"
	"github.com/qisanfen666/agentflow/storage"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	addr := flag.String("addr", env("AGENTFLOW_ADDR", ":8080"), "listen address")
	mode := env("AGENTFLOW_MODE", "memory")
	flag.Parse()

	gin.SetMode(gin.ReleaseMode)

	// 手工装配：model <- storage/runtime <- engine <- api（依赖方向单向）
	var (
		agents storage.AgentStore
		tasks  storage.TaskStore
		idem   storage.IdemStore
		tools  storage.ToolStore
		queue  engine.Queue
	)

	switch mode {
	case "redis":
		rdb := redis.NewClient(&redis.Options{
			Addr: env("AGENTFLOW_REDIS_ADDR", "localhost:6380"),
			DB:   intEnv("AGENTFLOW_REDIS_DB", 0),
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := rdb.Ping(ctx).Err(); err != nil {
			cancel()
			log.Fatalf("redis 不可达: %v", err)
		}
		cancel()

		agents = storage.NewRedisAgentStore(rdb)
		tasks = storage.NewRedisTaskStore(rdb)
		idem = storage.NewRedisIdemStore(rdb)
		tools = storage.NewRedisToolStore(rdb)

		visibility := time.Duration(intEnv("AGENTFLOW_VISIBILITY_SEC", 300)) * time.Second
		rq := engine.NewRedisQueue(rdb, visibility)
		stopMover := rq.Start(context.Background())
		defer stopMover()
		queue = rq
		log.Printf("storage=redis(%s db=%d) queue=redis visibility=%v", rdb.Options().Addr, rdb.Options().DB, visibility)
	default:
		agents = storage.NewMemoryAgentStore()
		tasks = storage.NewMemoryTaskStore()
		idem = storage.NewMemoryIdemStore()
		tools = storage.NewMemoryToolStore()
		queue = engine.NewMemoryQueue()
		log.Printf("storage=memory queue=memory")
	}

	hub := dispatch.NewHub()
	// 双 Runtime 共存：按 AgentSpec.Runtime.Type 路由。
	// 沙箱限制用固定演示值（只读根 + 256MB + 1 核）。
	dispatcher := dispatch.New(agents, tasks, hub,
		runtime.NewPythonHTTP(),
		runtime.NewDocker(runtime.SandboxOptions{
			ReadOnlyRootFS: true,
			MemoryMB:       256,
			NanoCPUs:       1_000_000_000,
		}),
	)
	worker := engine.NewWorker(queue, dispatcher, tasks, engine.WorkerConfig{})
	stop := worker.Start(context.Background())
	defer stop()

	r := api.NewRouter(api.Dependencies{
		Agents:     agents,
		Tasks:      tasks,
		Dispatcher: dispatcher,
		Hub:        hub,
		Queue:      queue,
		Idem:       idem,
		Tools:      registry.New(tools),
	})

	log.Printf("agentflow-server listening on %s (mode=%s, runtime=python-http)", *addr, mode)
	if err := r.Run(*addr); err != nil {
		log.Fatal(err)
	}
}

func intEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
