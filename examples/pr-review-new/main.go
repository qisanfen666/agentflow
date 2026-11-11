// PR Review 中台 demo（pr-review-new）：agentflow 嵌入形态 + 裸 agent 执行面。
//
// 验证命题：裸 agent（examples/pr-review-new/agent/agent.py，基础 LLM 调用 +
// read_file/write_file 工具循环，零依赖实现 SSE 合同）经标准合同接入后，
// 由控制面获得排队 / 审批 / 直播 / 回写 / 预算 / 审计全部生产化能力。
//
// 角色扮演（对照 pr-review-story.md）：
//   - 中台后端（本程序）：宿主 gin 应用，Mount 控制面；业务面 = CI webhook、
//     PR 注册表（假 PR 平台）、SSE 直播代理、结果回写、角色视角代理
//   - 控制面（agentflow）：被嵌入，治理（认证/限流/预算/审批）全部库原生能力
//   - 执行面（裸 agent）：独立 Python 进程，OpenAI 兼容流式 + mock 降级
//
// 运行（两个终端，mock 模式零外部依赖）：
//
//	终端1: python examples/pr-review-new/agent/agent.py
//	终端2: go run ./examples/pr-review-new        # GUI: http://localhost:8090
//
// 真实 LLM：终端1 先设 OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL。
// 解耦边界：本程序只 import agentflow 公开面（agentflow / model）；
// 执行面只认 SSE 合同，不感知控制面存在。
package main

import (
	"context"
	"embed"
	"encoding/json"
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
	// 只用内置 python-http runtime：裸 agent 走标准 SSE 合同直连——
	// 不需要任何自定义 Runtime，这正是"裸 agent 生产化"命题的验证点。
	panel, err := agentflow.New(agentflow.Config{
		Server:  agentflow.ServerConfig{Mode: gin.ReleaseMode}, // Addr 留空：监听归宿主
		Storage: cfg.storage,
		Runtimes: []string{"python-http"},
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
	})
	if err != nil {
		log.Fatalf("agentflow 装配失败: %v", err)
	}
	panel.Mount(r.Group("/agentflow"))

	cp := newCPClient("http://127.0.0.1:"+cfg.port+"/agentflow", cfg)
	mid := &midway{cfg: cfg, cp: cp, board: board}
	if cfg.githubToken != "" {
		mid.gh = newGithubClient(cfg.githubAPIBase, cfg.githubToken)
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
		api.GET("/audit", mid.auditTail)          // admin 专属：审计尾部
		api.GET("/metrics", mid.metricsProxy)     // admin 专属：指标代取
		api.GET("/agent-health", mid.agentHealth) // 裸 agent 状态
	}
	// CI 入口（身份固定 submitter；GitHub Actions 等调用，见 README）
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

	// CI 自动触发：轮询 GitHub 开放 PR（新 PR 自动评审 / 新提交自动重跑）
	go mid.runCIPoller(ctx)

	if cfg.configLoaded {
		log.Printf("配置文件: %s（同名环境变量可覆盖）", cfg.configPath)
	} else {
		log.Printf("配置文件: 未找到 %s，使用默认值（模板见 config.example.json）", cfg.configPath)
	}
	log.Printf("PR Review 中台: http://localhost:%s/ui  （裸 agent %s，存储 %s，审计 %s）",
		cfg.port, cfg.agentAddr, cfg.storage.Driver, cfg.auditPath)
	log.Printf("别忘了另一个终端: python examples/pr-review-new/agent/agent.py （未起则任务重试等待）")

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
//
// 三层优先级：环境变量 > config.json > 代码默认值。
// 配置文件路径：CONFIG 环境变量指定，默认 examples/pr-review-new/config.json
//（相对仓库根运行）。文件缺失不是错误——回退默认值，行为与无配置时一致。

// githubRepoCfg GitHub 远端仓库映射（github.repos[]，下一轮接入真实拉取）。
type githubRepoCfg struct {
	Name  string `json:"name"` // demo 内显示名 / PR 注册表 key
	Owner string `json:"owner"`
	Repo  string `json:"repo"`
	Prod  bool   `json:"prod"`
}

type configFile struct {
	LLM struct {
		BaseURL    string `json:"base_url"` // agent 消费；中台仅透传展示
		APIKey     string `json:"api_key"`
		Model      string `json:"model"`
		TimeoutSec int    `json:"timeout_sec"`
	} `json:"llm"`
	GitHub struct {
		APIBase      string          `json:"api_base"`
		Token        string          `json:"token"`
		Repos        []githubRepoCfg `json:"repos"`
		PollSec      int             `json:"poll_sec"`      // CI 轮询间隔秒（0=关）
		PostComments *bool           `json:"post_comments"` // 结论回写 PR 评论（默认开）
		BranchPrefix string          `json:"branch_prefix"` // 只自动评审该前缀分支（空=全部）
	} `json:"github"`
	Midway struct {
		Port       string `json:"port"`
		AgentAddr  string `json:"agent_addr"`
		AdminKey   string `json:"admin_key"`
		SubmitKey  string `json:"submit_key"`
		ReaderKey  string `json:"reader_key"`
		Storage    string `json:"storage"`
		RedisAddr  string `json:"redis_addr"`
		AuditPath  string `json:"audit_path"`
		RateLimit  int    `json:"rate_limit_per_min"`
		DailyBudget int64 `json:"daily_token_budget"`
	} `json:"midway"`
}

type envConfig struct {
	port        string
	agentAddr   string // 裸 agent 地址（host:port）
	storage     agentflow.StorageConfig
	adminKey    string
	submitKey   string
	readerKey   string
	rateLimit   int
	dailyBudget int64
	auditPath   string
	demoRoot    string // testdata 解析基点（默认仓库根运行）

	configPath    string // 生效的配置文件（空=未找到，全默认）
	configLoaded  bool
	githubAPIBase string
	githubToken   string
	githubRepos   []githubRepoCfg
	ciPollSec     int    // CI 轮询间隔秒（0=关）
	ghPostComments bool  // 评审结论回写 GitHub PR 评论
	ghBranchPrefix string // CI 只自动评审该前缀分支
}

func configPath() string {
	if p := os.Getenv("CONFIG"); p != "" {
		return p
	}
	return filepath.Join("examples", "pr-review-new", "config.json")
}

func readConfigFile(path string) (configFile, bool) {
	var cf configFile
	raw, err := os.ReadFile(path)
	if err != nil {
		return cf, false
	}
	if err := json.Unmarshal(raw, &cf); err != nil {
		log.Fatalf("配置文件 %s 解析失败: %v", path, err)
	}
	return cf, true
}

func loadEnv() envConfig {
	path := configPath()
	cf, ok := readConfigFile(path)
	mc, gh := cf.Midway, cf.GitHub

	cfg := envConfig{
		configPath:     path,
		configLoaded:   ok,
		githubAPIBase:  pickStr(envOr("GITHUB_API_BASE", ""), gh.APIBase, "https://api.github.com"),
		githubToken:    pickStr(os.Getenv("GITHUB_TOKEN"), gh.Token, ""),
		githubRepos:    gh.Repos,
		ciPollSec:      pickInt(envOr("CI_POLL_SEC", ""), gh.PollSec, 15),
		ghPostComments: pickBool(os.Getenv("CI_POST_COMMENTS"), gh.PostComments, true),
		ghBranchPrefix: pickStr(os.Getenv("CI_BRANCH_PREFIX"), gh.BranchPrefix, ""),
		demoRoot:       envOr("DEMO_ROOT", "."),
	}

	cfg.port = pickStr(envOr("PORT", ""), mc.Port, "8090")
	cfg.agentAddr = pickStr(envOr("AGENT_ADDR", ""), mc.AgentAddr, "127.0.0.1:8091")
	cfg.adminKey = pickStr(envOr("ADMIN_KEY", ""), mc.AdminKey, "admin-key")
	cfg.submitKey = pickStr(envOr("SUBMIT_KEY", ""), mc.SubmitKey, "submit-key")
	cfg.readerKey = pickStr(envOr("READER_KEY", ""), mc.ReaderKey, "reader-key")
	cfg.rateLimit = pickInt(envOr("RATE_LIMIT_PER_MIN", ""), mc.RateLimit, 60)
	cfg.dailyBudget = pickInt64(envOr("DAILY_TOKEN_BUDGET", ""), mc.DailyBudget, 5_000_000)
	cfg.auditPath = pickStr(envOr("AUDIT_PATH", ""), mc.AuditPath, filepath.Join("tmp", "pr-new-audit.jsonl"))

	switch pickStr(strings.ToLower(os.Getenv("STORAGE")), strings.ToLower(mc.Storage), "memory") {
	case "redis":
		addr := pickStr(envOr("REDIS_ADDR", ""), mc.RedisAddr, "localhost:6379")
		cfg.storage = agentflow.StorageConfig{Driver: "redis", Redis: agentflow.RedisConfig{Addr: addr}}
	default:
		cfg.storage = agentflow.StorageConfig{Driver: "memory"}
	}
	return cfg
}

// pickStr 三层取值：env 显式值 > 文件值 > 默认值（空串视为未设置）。
func pickStr(envVal, fileVal, def string) string {
	if envVal != "" {
		return envVal
	}
	if fileVal != "" {
		return fileVal
	}
	return def
}

// envOr 取环境变量，未设置返回 def。
func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func pickInt(envVal string, fileVal, def int) int {
	if n, ok := atoi(envVal); ok {
		return n
	}
	if fileVal > 0 {
		return fileVal
	}
	return def
}

func pickInt64(envVal string, fileVal, def int64) int64 {
	if n, ok := atoi(envVal); ok {
		return int64(n)
	}
	if fileVal > 0 {
		return fileVal
	}
	return def
}

// pickBool 三层取值：env("1|true") > 文件指针 > 默认。
func pickBool(envVal string, fileVal *bool, def bool) bool {
	switch strings.ToLower(envVal) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	if fileVal != nil {
		return *fileVal
	}
	return def
}

func atoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, false
	}
	return n, true
}
