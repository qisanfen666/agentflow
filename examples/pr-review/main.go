// PR Review 中台 demo：agentflow 嵌入形态的完整场景应用。
//
// 角色扮演（对照 tmp/pr-review-story.md）：
//   - 中台后端（本程序）：宿主 gin 应用，Mount 控制面；自身业务面 = CI webhook、
//     PR 注册表（假 PR 平台）、SSE 直播代理、结果回写
//   - 控制面（agentflow）：被嵌入，负责排队/审批/沙箱/审计/预算——全部库原生能力
//   - 执行面（claude-code Runtime）：一次性容器内跑 claude CLI，stream-json 转事件流
//
// 运行（默认 mock 模式，无需 API key）:
//
//	go run ./examples/pr-review          # GUI: http://localhost:8090
//
// 真实模式: CLAUDE_IMAGE=agentflow-claude:latest ANTHROPIC_API_KEY=sk-... 同上
// 环境变量: PORT / STORAGE(memory|redis) / REDIS_ADDR / DEMO_ROOT / 审计路径见 audit()
//
// 解耦边界：本程序只 import agentflow 公开面（agentflow / runtime / model）+
// demo 私有的 internal/claude；不触碰库 internal/**。
package main

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	agentflow "github.com/qisanfen666/agentflow"
	"github.com/qisanfen666/agentflow/examples/pr-review/internal/claude"
)

//go:embed web
var webFS embed.FS

func main() {
	gin.SetMode(gin.ReleaseMode) // 先于一切路由注册（否则 setup 期仍打 debug 路由表）
	cfg := loadEnv()

	// ---------- 宿主业务面 ----------
	r := gin.New()
	r.Use(gin.Recovery(), gin.LoggerWithWriter(log.Writer()))

	board := newBoard(cfg)

	// ---------- 控制面装配（嵌入形态） ----------
	// 执行模式三选一（互斥，按环境变量）：
	//   默认           mock：回放录制会话，无需 key/docker 之外的东西
	//   CLAUDE_IMAGE   容器：一次性 docker 跑 claude（强隔离，生产形态）
	//   CLAUDE_LOCAL=1 本地：直接 exec 宿主机 claude（无隔离；只读评审/开发验证用）
	image := envOr("CLAUDE_IMAGE", claude.DefaultMockImage)
	rtOpts := claude.Options{Image: image}
	if os.Getenv("CLAUDE_LOCAL") != "" {
		rtOpts = claude.Options{LocalBin: envOr("CLAUDE_BIN", "claude")}
	}
	panel, err := agentflow.New(agentflow.Config{
		Server:  agentflow.ServerConfig{Mode: gin.ReleaseMode}, // Addr 留空：监听归宿主
		Storage: cfg.storage,
		Runtimes: []string{}, // 只用自定义 claude-code（内置开关清空）
		Auth: agentflow.AuthConfig{
			Enabled: true,
			Keys: []agentflow.APIKey{
				{Key: cfg.adminKey, Roles: []string{"admin"}},
				{Key: cfg.submitKey, Roles: []string{"submitter"}},
				{Key: cfg.readerKey, Roles: []string{"reader"}},
			},
		},
		Governance: agentflow.GovernanceConfig{
			RateLimitPerMin:  cfg.rateLimit,
			DailyTokenBudget: cfg.dailyBudget,
		},
		Observability: agentflow.ObservabilityConfig{
			MetricsEnabled: true,
			AuditLogPath:   cfg.auditPath,
			ServiceName:    "pr-review-midway",
		},
	}, agentflow.WithRuntime(claude.New(rtOpts)))
	if err != nil {
		log.Fatalf("agentflow 装配失败: %v", err)
	}
	panel.Mount(r.Group("/agentflow"))

	cp := newCPClient("http://127.0.0.1:"+cfg.port+"/agentflow", cfg)
	mid := &midway{cfg: cfg, cp: cp, board: board}
	mid.mode = "mock"
	if rtOpts.LocalBin != "" {
		mid.mode = "local"
	} else if image != claude.DefaultMockImage {
		mid.mode = "real"
	}

	// ---------- 中台业务路由 ----------
	api := r.Group("/api")
	{
		api.GET("/board", mid.getBoard)
		api.POST("/board/prs", mid.createPR)
		api.POST("/board/prs/:num/submit", mid.submitReview)
		api.POST("/board/prs/:num/approve", mid.approve)
		api.POST("/board/prs/:num/reject", mid.reject)
		api.GET("/board/prs/:num/events", mid.streamEvents)
		api.GET("/metrics", mid.metricsProxy) // /metrics 仅 admin：GUI 经中台代取
	}
	// CI 入口（预留：GitHub Actions / 其它 CI 调这里，见 cicd/）
	r.POST("/webhook/pr", mid.webhookPR)

	// 控制台静态资源（go:embed，无外部依赖）
	sub, _ := fs.Sub(webFS, "web")
	r.StaticFS("/ui", http.FS(sub))
	r.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/ui/") })
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 生命周期顺序：先起宿主监听（loopback 客户端要能连通）-> 控制面后台 ->
	// 幂等 bootstrap（走公开 API 注册 Agent）-> 对外宣告就绪。
	gin.SetMode(gin.ReleaseMode)
	srv := &http.Server{Addr: ":" + cfg.port, Handler: r}
	lnErr := make(chan error, 1)
	go func() { lnErr <- srv.ListenAndServe() }()
	if err := waitPort("127.0.0.1:" + cfg.port); err != nil {
		log.Fatalf("监听 %s 失败: %v", cfg.port, err)
	}

	if err := panel.Start(ctx); err != nil {
		log.Fatalf("控制面启动失败: %v", err)
	}

	// 阶段 0：管理员初始化（幂等）——注册评审 Agent + 记录 agent_id
	if err := mid.bootstrapAgents(ctx); err != nil {
		log.Fatalf("Agent 初始化失败: %v", err)
	}
	mode := "mock（无需 API key，回放录制会话）"
	switch {
	case rtOpts.LocalBin != "":
		mode = "local（宿主机直接运行 claude，无容器隔离）"
	case image != claude.DefaultMockImage:
		mode = "real（一次性容器内运行 claude CLI）"
	}
	execDesc := "镜像 " + image
	if rtOpts.LocalBin != "" {
		execDesc = "本地 claude (" + rtOpts.LocalBin + ")"
	}
	log.Printf("PR Review 中台: http://localhost:%s/ui  （%s，模式 %s；审计 %s）",
		cfg.port, execDesc, mode, cfg.auditPath)

	<-ctx.Done()
	log.Println("退出信号，关停中台...")
	select { // 监听早已失败的话这里把真实错误带出来
	case err := <-lnErr:
		if err != nil && err != http.ErrServerClosed {
			log.Printf("listen: %v", err)
		}
	default:
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = panel.Stop(shutdownCtx)
}

// waitPort 等端口可连（给 loopback 客户端一个就绪屏障）。
func waitPort(addr string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
		if err == nil {
			return conn.Close()
		}
		time.Sleep(150 * time.Millisecond)
	}
	return fmt.Errorf("端口 %s 未就绪", addr)
}

// ---------- 环境与配置 ----------

type envConfig struct {
	port        string
	storage     agentflow.StorageConfig
	adminKey    string
	submitKey   string
	readerKey   string
	rateLimit   int
	dailyBudget int64
	auditPath   string
	demoRoot    string // testdata 根（仓库相对路径解析基点）
}

func loadEnv() envConfig {
	cfg := envConfig{
		port:      envOr("PORT", "8090"),
		adminKey:  envOr("ADMIN_KEY", "admin-key"),
		submitKey: envOr("SUBMIT_KEY", "submit-key"),
		readerKey: envOr("READER_KEY", "reader-key"),
		rateLimit: envOrInt("RATE_LIMIT_PER_MIN", 60),
		dailyBudget: envOrInt64("DAILY_TOKEN_BUDGET", 5_000_000),
		auditPath: envOr("AUDIT_PATH", filepath.Join("tmp", "pr-audit.jsonl")),
		demoRoot:  envOr("DEMO_ROOT", "."),
	}
	switch strings.ToLower(os.Getenv("STORAGE")) {
	case "redis":
		addr := envOr("REDIS_ADDR", "localhost:6379")
		cfg.storage = agentflow.StorageConfig{Driver: "redis", Redis: agentflow.RedisConfig{Addr: addr}}
	default:
		cfg.storage = agentflow.StorageConfig{Driver: "memory"}
	}
	return cfg
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envOrInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

func envOrInt64(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}
