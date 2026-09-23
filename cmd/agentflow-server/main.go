// agentflow-server 是开箱即用的独立部署二进制：facade 的最薄装配示例。
// 全部装配逻辑在根包 Panel（agentflow.New），本文件只做配置映射与信号处理。
//
// 环境变量：
//
//	AGENTFLOW_MODE           memory | redis（默认 memory）
//	AGENTFLOW_ADDR           监听地址（默认 :8080）
//	AGENTFLOW_REDIS_ADDR     Redis 地址（默认 localhost:6380）
//	AGENTFLOW_REDIS_DB       Redis DB（默认 0）
//	AGENTFLOW_VISIBILITY_SEC 在途租约/可见性超时秒数（默认 300）
//	AGENTFLOW_AUDIT_LOG_PATH 审计日志路径（空 = 关闭审计）
//	AGENTFLOW_METRICS_ENABLED 置 1 启用 /metrics（Prometheus）
//	AGENTFLOW_SERVICE_NAME    OTel 服务名（默认 agentflow）
//	AGENTFLOW_OTLP_ENDPOINT   OTLP gRPC 地址（如 localhost:4317；空 = 关闭追踪）
//	AGENTFLOW_AUTH_ENABLED    置 1 启用认证授权
//	AGENTFLOW_API_KEYS        key 列表，格式 "key1:admin;key2:submitter,reader"
//	AGENTFLOW_RATE_LIMIT_PER_MIN 全局提交速率上限/分钟（0 = 不限）
//	AGENTFLOW_TOKEN_BUDGET    自然日 token 预算（0 = 不限）
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	agentflow "github.com/qisanfen666/agentflow"
	"github.com/qisanfen666/agentflow/internal/api"
	"github.com/qisanfen666/agentflow/model"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// buildAuthConfig 从 env 装配认证配置。
func buildAuthConfig() agentflow.AuthConfig {
	cfg := agentflow.AuthConfig{Enabled: env("AGENTFLOW_AUTH_ENABLED", "") == "1"}
	for _, e := range api.ParseAPIKeys(env("AGENTFLOW_API_KEYS", "")) {
		cfg.Keys = append(cfg.Keys, agentflow.APIKey{Key: e.Key, Roles: e.Roles})
	}
	return cfg
}

// int64Env 环境变量取 int64（预算等大数配置用）。
func int64Env(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func main() {
	addr := flag.String("addr", env("AGENTFLOW_ADDR", ":8080"), "listen address")
	flag.Parse()

	// 双 Runtime（直连 + 沙箱），沙箱用固定资源限制
	panel, err := agentflow.New(agentflow.Config{
		Server: agentflow.ServerConfig{Addr: *addr, Mode: gin.ReleaseMode},
		Storage: agentflow.StorageConfig{
			Driver: env("AGENTFLOW_MODE", "memory"),
			Redis: agentflow.RedisConfig{
				Addr: env("AGENTFLOW_REDIS_ADDR", "localhost:6380"),
				DB:   intEnv("AGENTFLOW_REDIS_DB", 0),
			},
		},
		Queue: agentflow.QueueConfig{
			VisibilityTimeoutSec: intEnv("AGENTFLOW_VISIBILITY_SEC", 300),
		},
		Observability: agentflow.ObservabilityConfig{
			AuditLogPath:   env("AGENTFLOW_AUDIT_LOG_PATH", ""),
			MetricsEnabled: env("AGENTFLOW_METRICS_ENABLED", "") == "1",
			ServiceName:    env("AGENTFLOW_SERVICE_NAME", "agentflow"),
			OTLPEndpoint:   env("AGENTFLOW_OTLP_ENDPOINT", ""),
		},
		Auth: buildAuthConfig(),
		Governance: agentflow.GovernanceConfig{
			RateLimitPerMin:  intEnv("AGENTFLOW_RATE_LIMIT_PER_MIN", 0),
			DailyTokenBudget: int64Env("AGENTFLOW_TOKEN_BUDGET", 0),
		},
		Runtimes: []string{model.RuntimePythonHTTP, model.RuntimeDocker},
		Sandbox: agentflow.SandboxConfig{
			ReadOnlyRootFS: true,
			MemoryMB:       256,
			NanoCPUs:       1_000_000_000,
		},
	})
	if err != nil {
		log.Fatalf("装配失败: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := panel.Start(ctx); err != nil {
		log.Fatalf("启动失败: %v", err)
	}
	log.Printf("agentflow-server listening on %s (storage=%s, runtimes=python-http,docker)", *addr, panel.Config().Storage.Driver)

	// 阻塞等信号，收到后优雅关停（在途任务经 Nack 回队，重启续跑）
	<-ctx.Done()
	log.Println("收到退出信号，开始优雅关停...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := panel.Stop(shutdownCtx); err != nil {
		log.Printf("关停异常: %v", err)
	}
	log.Println("已退出")
}
