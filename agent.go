// Panel 是控制面库的门面：一次 New 完成全部装配，Start/Stop 管理生命周期，
// Router/Mount 提供两种接入形态（独立服务 / 嵌入既有 gin 应用）。
//
// 典型用法：
//
//	panel, err := agentflow.New(agentflow.Config{
//	    Storage:  agentflow.StorageConfig{Driver: "redis", Redis: agentflow.RedisConfig{Addr: "localhost:6380"}},
//	    Runtimes: []string{"python-http", "docker"},
//	})
//	if err != nil { ... }
//	panel.Start(ctx)
//	defer panel.Stop(ctx)
package agentflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/qisanfen666/agentflow/internal/api"
	"github.com/qisanfen666/agentflow/internal/dispatch"
	"github.com/qisanfen666/agentflow/internal/engine"
	"github.com/qisanfen666/agentflow/internal/observability"
	"github.com/qisanfen666/agentflow/internal/registry"
	"github.com/qisanfen666/agentflow/model"
	"github.com/qisanfen666/agentflow/runtime"
	"github.com/qisanfen666/agentflow/storage"
)

// Panel 持有装配完成的全部组件。零值不可用，必须经 New 构建。
type Panel struct {
	cfg   Config
	deps  api.Dependencies
	route *gin.Engine

	worker     *engine.Worker
	redisQueue *engine.RedisQueue // Redis 模式下非 nil，Start 时启动搬运协程
	rdb        *redis.Client

	httpSrv *http.Server

	auditCloser io.Closer // 审计日志文件句柄，Stop 时关闭

	mu         sync.Mutex
	started    bool
	stopped    bool
	runCancel  context.CancelFunc // worker/mover 的生命周期根
	runCtx     context.Context
	stopWorker func() // 停止并等待 worker 全部协程
	stopMover  func() // 停止 delayed->ready 搬运协程（Redis 模式）
}

// New 按配置装配控制面。只做构建与连通性检查，不启动任何 goroutine——
// 便于在 main 里尽早暴露配置错误（存不连不上、Runtime 名写错等）。
func New(cfg Config) (*Panel, error) {
	p := &Panel{cfg: withDefaults(cfg)}

	var (
		agents storage.AgentStore
		tasks  storage.TaskStore
		idem   storage.IdemStore
		tools  storage.ToolStore
		queue  engine.Queue
	)

	switch p.cfg.Storage.Driver {
	case driverMemory:
		agents = storage.NewMemoryAgentStore()
		tasks = storage.NewMemoryTaskStore()
		idem = storage.NewMemoryIdemStore()
		tools = storage.NewMemoryToolStore()
		queue = engine.NewMemoryQueue()
	case driverRedis:
		rdb := redis.NewClient(&redis.Options{
			Addr:     p.cfg.Storage.Redis.Addr,
			Password: p.cfg.Storage.Redis.Password,
			DB:       p.cfg.Storage.Redis.DB,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := rdb.Ping(ctx).Err()
		cancel()
		if err != nil {
			_ = rdb.Close()
			return nil, fmt.Errorf("storage: redis 不可达: %w", err)
		}
		visibility := time.Duration(p.cfg.Queue.VisibilityTimeoutSec) * time.Second
		rq := engine.NewRedisQueue(rdb, visibility)
		p.rdb = rdb
		p.redisQueue = rq
		agents = storage.NewRedisAgentStore(rdb)
		tasks = storage.NewRedisTaskStore(rdb)
		idem = storage.NewRedisIdemStore(rdb)
		tools = storage.NewRedisToolStore(rdb)
		queue = rq
	default:
		return nil, fmt.Errorf("storage: 未知驱动 %q（可选 memory | redis）", p.cfg.Storage.Driver)
	}

	// Runtime 装配：按声明列表构建，重复/未知类型直接报错（宁可启动失败，不要静默降级）。
	runtimes, err := buildRuntimes(p.cfg)
	if err != nil {
		return nil, err
	}

	// 审计：配置路径即启用；文件打不开属启动失败，不带病运行
	var audit observability.AuditLogger
	if path := p.cfg.Observability.AuditLogPath; path != "" {
		fl, err := observability.NewFileAuditLogger(path)
		if err != nil {
			return nil, fmt.Errorf("observability: 审计日志不可写 %s: %w", path, err)
		}
		p.auditCloser = fl
		audit = fl
	}

	hub := dispatch.NewHub()
	dispatcher := dispatch.New(agents, tasks, hub, audit, runtimes...)
	reg := registry.New(tools)
	worker := engine.NewWorker(queue, dispatcher, tasks, engine.WorkerConfig{
		MaxRetries:       p.cfg.Queue.MaxRetries,
		RetryBackoffBase: time.Duration(p.cfg.Queue.RetryBackoffBaseMs) * time.Millisecond,
	})

	p.worker = worker
	p.deps = api.Dependencies{
		Agents:     agents,
		Tasks:      tasks,
		Dispatcher: dispatcher,
		Hub:        hub,
		Queue:      queue,
		Idem:       idem,
		Tools:      reg,
		Audit:      audit,
	}
	p.route = api.NewRouter(p.deps)
	return p, nil
}

// Start 启动后台组件：任务 worker（+ 已在 New 中启动的 delayed 搬运协程）。
// 若配置了 Server.Addr，同时开始监听 HTTP；为空则只跑后台（嵌入模式由宿主挂载路由）。
func (p *Panel) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return errors.New("agentflow: Panel 已停止，不可重启")
	}
	if p.started {
		return nil
	}
	p.started = true

	if p.cfg.Server.Mode != "" {
		gin.SetMode(p.cfg.Server.Mode)
	}
	// 生命周期根 ctx：派生自调用方 ctx（取消会传导），Stop 时统一回收
	p.runCtx, p.runCancel = context.WithCancel(ctx)
	if p.redisQueue != nil {
		p.stopMover = p.redisQueue.Start(p.runCtx)
	}
	p.stopWorker = p.worker.Start(p.runCtx)

	if addr := p.cfg.Server.Addr; addr != "" {
		p.httpSrv = &http.Server{Addr: addr, Handler: p.route}
		go func() {
			if err := p.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				// 监听失败属致命错误：停止后台组件，避免半启动状态
				_ = p.Stop(context.Background())
			}
		}()
	}
	return nil
}

// Stop 优雅关停。顺序：HTTP（拒新请求）-> worker（停止取新任务）
// -> delayed 搬运协程 -> Redis 连接。
// 在途任务不强等：其 ctx 随 worker 取消，任务经 Nack 回队，由下次启动
// 依靠可见性超时回收续跑——这是 at-least-once 语义下的标准取舍。
// 幂等：多次调用安全。
func (p *Panel) Stop(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil
	}
	p.stopped = true

	var firstErr error
	if p.httpSrv != nil {
		if err := p.httpSrv.Shutdown(ctx); err != nil {
			firstErr = fmt.Errorf("http shutdown: %w", err)
		}
	}
	if p.stopWorker != nil {
		p.stopWorker() // cancel + 等待全部消费/回收协程退出
	}
	if p.stopMover != nil {
		p.stopMover()
	}
	if p.runCancel != nil {
		p.runCancel()
	}
	if p.auditCloser != nil {
		_ = p.auditCloser.Close()
	}
	if p.rdb != nil {
		_ = p.rdb.Close()
	}
	return firstErr
}

// Router 返回装配完成的 gin 引擎，供调用方自行托管（自定义端口/TLS/中间件）。
// 与 Mount 二选一；Router 返回的引擎与 Start 内部监听用的是同一个。
func (p *Panel) Router() *gin.Engine { return p.route }

// Config 返回生效配置（默认值填充后的最终形态，排查配置问题用）。
func (p *Panel) Config() Config { return p.cfg }

// Mount 把控制面端点注册进既有的 router group（嵌入模式）：
//
//	r := gin.Default()
//	panel.Mount(r.Group("/agentflow"))
//
// 组内包含 /health 与全部 /api/v1/* 端点。
func (p *Panel) Mount(rg *gin.RouterGroup) { api.MountRoutes(rg, p.deps) }

// ---------- 内部装配 ----------

const (
	driverMemory = "memory"
	driverRedis  = "redis"
)

// withDefaults 填充零值默认。默认值集中在此，配置字段注释保持单一事实源。
func withDefaults(cfg Config) Config {
	if cfg.Storage.Driver == "" {
		cfg.Storage.Driver = driverMemory
	}
	if cfg.Storage.Driver == driverRedis && cfg.Storage.Redis.Addr == "" {
		cfg.Storage.Redis.Addr = "localhost:6379"
	}
	if cfg.Runtimes == nil {
		cfg.Runtimes = []string{model.RuntimePythonHTTP}
	}
	if cfg.Queue.MaxRetries == 0 {
		cfg.Queue.MaxRetries = 3
	}
	if cfg.Queue.RetryBackoffBaseMs == 0 {
		cfg.Queue.RetryBackoffBaseMs = 1000
	}
	if cfg.Queue.VisibilityTimeoutSec == 0 {
		cfg.Queue.VisibilityTimeoutSec = 300
	}
	return cfg
}

// buildRuntimes 按声明列表构建执行环境。未知类型报错而非忽略：
// 配置里写了 "docer"（拼错）应当启动失败，而不是运行到路由时才发现。
func buildRuntimes(cfg Config) ([]runtime.Runtime, error) {
	seen := make(map[string]bool, len(cfg.Runtimes))
	out := make([]runtime.Runtime, 0, len(cfg.Runtimes))
	for _, t := range cfg.Runtimes {
		if seen[t] {
			return nil, fmt.Errorf("runtimes: 重复声明 %q", t)
		}
		seen[t] = true
		switch t {
		case model.RuntimePythonHTTP:
			out = append(out, runtime.NewPythonHTTP())
		case model.RuntimeDocker:
			out = append(out, runtime.NewDocker(runtime.SandboxOptions{
				ReadOnlyRootFS: cfg.Sandbox.ReadOnlyRootFS,
				MemoryMB:       cfg.Sandbox.MemoryMB,
				NanoCPUs:       cfg.Sandbox.NanoCPUs,
			}))
		default:
			return nil, fmt.Errorf("runtimes: 未知类型 %q（可选 python-http | docker）", t)
		}
	}
	return out, nil
}
