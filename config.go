// Package agentflow 是 AgentFlow 控制面库的入口包。
//
// 设计定位：可 import 的 Agent 控制面库，只做调度/排队/隔离/审计，
// 不提供 AI 能力。本文件定义用户可见的配置契约。
package agentflow

// Config 是用户唯一需要接触的顶层配置。
// 六大配置块与 internal 各组件一一对应，互不嵌套职责。
type Config struct {
	Server        ServerConfig        `yaml:"server" json:"server"`
	Storage       StorageConfig       `yaml:"storage" json:"storage"`
	Queue         QueueConfig         `yaml:"queue" json:"queue"`
	Runtimes      []string            `yaml:"runtimes" json:"runtimes"` // 启用的执行环境，默认 ["python-http"]；docker 需显式加并配 Sandbox
	Sandbox       SandboxConfig       `yaml:"sandbox" json:"sandbox"`
	Auth          AuthConfig          `yaml:"auth" json:"auth"`
	Governance    GovernanceConfig    `yaml:"governance" json:"governance"`
	Observability ObservabilityConfig `yaml:"observability" json:"observability"`
}

// GovernanceConfig 控制提交前治理（M6）：限流与成本护栏。零值 = 全部关闭。
type GovernanceConfig struct {
	RateLimitPerMin  int   `yaml:"rate_limit_per_min" json:"rate_limit_per_min"` // 全局提交速率上限/分钟；0 = 不限
	DailyTokenBudget int64 `yaml:"daily_token_budget" json:"daily_token_budget"` // 自然日 token 预算（按实际 usage 扣减）；0 = 不限
}

// ServerConfig 控制 API 层。
type ServerConfig struct {
	Addr string `yaml:"addr" json:"addr"` // 监听地址，默认 ":8080"
	Mode string `yaml:"mode" json:"mode"` // gin mode: debug | release | test
}

// StorageConfig 选择存储实现：memory（默认，零依赖）或 redis。
type StorageConfig struct {
	Driver string      `yaml:"driver" json:"driver"` // memory | redis
	Redis  RedisConfig `yaml:"redis" json:"redis"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr" json:"addr"`  // 例 "localhost:6379"
	Password string `yaml:"password" json:"-"` // 序列化时隐藏
	DB       int    `yaml:"db" json:"db"`
}

// QueueConfig 控制任务队列与重试行为（内存实现 / Redis 实现）。
type QueueConfig struct {
	Name                 string `yaml:"name" json:"name"`                                     // 队列名，默认 "agentflow:tasks"
	MaxRetries           int    `yaml:"max_retries" json:"max_retries"`                       // 可重试错误的最大重试次数，默认 3
	RetryBackoffBaseMs   int    `yaml:"retry_backoff_base_ms" json:"retry_backoff_base_ms"`   // 指数退避基数，默认 1000
	VisibilityTimeoutSec int    `yaml:"visibility_timeout_sec" json:"visibility_timeout_sec"` // 任务被取走后多久未 Ack 视为丢失，默认 300
}

// SandboxConfig 控制 Docker 沙箱执行环境。
type SandboxConfig struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`                     // 是否启用 docker runtime
	Image           string `yaml:"image" json:"image"`                         // 沙箱基础镜像
	ReadOnlyRootFS  bool   `yaml:"read_only_root_fs" json:"read_only_root_fs"` // 根文件系统只读
	MemoryMB        int64  `yaml:"memory_mb" json:"memory_mb"`                 // 内存上限
	NanoCPUs        int64  `yaml:"nano_cpus" json:"nano_cpus"`                 // CPU 上限（1 核 = 1e9）
	NetworkDisabled bool   `yaml:"network_disabled" json:"network_disabled"`   // 禁用网络
}

// AuthConfig 控制访问认证与授权（M6）。
// 认证：X-API-Key 头匹配已配置 key；授权：key 绑定角色，方法级权限矩阵。
// 嵌入形态下宿主可不用内置实现，直接注入自己的鉴权中间件（Dependencies.Auth）。
type AuthConfig struct {
	Enabled bool     `yaml:"enabled" json:"enabled"`
	Keys    []APIKey `yaml:"keys" json:"keys"`
}

// APIKey 一把 key 及其角色。角色：admin（全权）/ submitter（提交+查任务+读配置）/ reader（只读）。
type APIKey struct {
	Key   string   `yaml:"key" json:"key"`
	Roles []string `yaml:"roles" json:"roles"`
}

// ObservabilityConfig 控制可观测性。
type ObservabilityConfig struct {
	ServiceName    string `yaml:"service_name" json:"service_name"`       // OTel service name
	OTLPEndpoint   string `yaml:"otlp_endpoint" json:"otlp_endpoint"`     // OTLP 导出地址
	MetricsEnabled bool   `yaml:"metrics_enabled" json:"metrics_enabled"` // Prometheus 指标
	AuditLogPath   string `yaml:"audit_log_path" json:"audit_log_path"`   // append-only 审计日志路径
}
