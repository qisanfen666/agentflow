// Package agentflow 是 AgentFlow 控制面库的入口包。
//
// 设计定位：可 import 的 Agent 控制面库，只做调度/排队/隔离/审计，
// 不提供 AI 能力。本文件只定义用户可见的配置契约（M0 定稿稿），
// 配置的加载（YAML + 环境变量覆盖）在 M1 实现。
package agentflow

// Config 是用户唯一需要接触的顶层配置。
// 六大配置块与 internal 各组件一一对应，互不嵌套职责。
type Config struct {
	Server        ServerConfig        `yaml:"server" json:"server"`
	Storage       StorageConfig       `yaml:"storage" json:"storage"`
	Queue         QueueConfig         `yaml:"queue" json:"queue"`
	Sandbox       SandboxConfig       `yaml:"sandbox" json:"sandbox"`
	Auth          AuthConfig          `yaml:"auth" json:"auth"`
	Observability ObservabilityConfig `yaml:"observability" json:"observability"`
}

// ServerConfig 控制 API 层。
type ServerConfig struct {
	Addr string `yaml:"addr" json:"addr"` // 监听地址，默认 ":8080"
	Mode string `yaml:"mode" json:"mode"` // gin mode: debug | release | test
}

// StorageConfig 选择存储实现：memory（默认，零依赖）或 redis（M2 引入）。
type StorageConfig struct {
	Driver string      `yaml:"driver" json:"driver"` // memory | redis
	Redis  RedisConfig `yaml:"redis" json:"redis"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr" json:"addr"`  // 例 "localhost:6379"
	Password string `yaml:"password" json:"-"` // 序列化时隐藏
	DB       int    `yaml:"db" json:"db"`
}

// QueueConfig 控制任务队列与重试行为（M1 内存实现 / M2 Redis 实现）。
type QueueConfig struct {
	Name                 string `yaml:"name" json:"name"`                                     // 队列名，默认 "agentflow:tasks"
	MaxRetries           int    `yaml:"max_retries" json:"max_retries"`                       // 可重试错误的最大重试次数，默认 3
	RetryBackoffBaseMs   int    `yaml:"retry_backoff_base_ms" json:"retry_backoff_base_ms"`   // 指数退避基数，默认 1000
	VisibilityTimeoutSec int    `yaml:"visibility_timeout_sec" json:"visibility_timeout_sec"` // 任务被取走后多久未 Ack 视为丢失，默认 300
}

// SandboxConfig 控制 Docker 沙箱执行环境（M3 生效）。
type SandboxConfig struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`                     // 是否启用 docker runtime
	Image           string `yaml:"image" json:"image"`                         // 沙箱基础镜像
	ReadOnlyRootFS  bool   `yaml:"read_only_root_fs" json:"read_only_root_fs"` // 根文件系统只读
	MemoryMB        int64  `yaml:"memory_mb" json:"memory_mb"`                 // 内存上限
	NanoCPUs        int64  `yaml:"nano_cpus" json:"nano_cpus"`                 // CPU 上限（1 核 = 1e9）
	NetworkDisabled bool   `yaml:"network_disabled" json:"network_disabled"`   // 禁用网络
}

// AuthConfig 控制访问认证（v1 仅 API Key；RBAC 属于 M6）。
type AuthConfig struct {
	Enabled bool     `yaml:"enabled" json:"enabled"`
	APIKeys []string `yaml:"api_keys" json:"api_keys"`
}

// ObservabilityConfig 控制可观测性（M5 生效）。
type ObservabilityConfig struct {
	ServiceName    string `yaml:"service_name" json:"service_name"`       // OTel service name
	OTLPEndpoint   string `yaml:"otlp_endpoint" json:"otlp_endpoint"`     // OTLP 导出地址
	MetricsEnabled bool   `yaml:"metrics_enabled" json:"metrics_enabled"` // Prometheus 指标
	AuditLogPath   string `yaml:"audit_log_path" json:"audit_log_path"`   // append-only 审计日志路径
}
