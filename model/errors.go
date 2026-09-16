package model

// 错误码与错误分类。唯一权威来源：docs/contracts/error-taxonomy.md。
// 全项目禁止散落字符串字面量，必须引用本文件常量。

// 错误码。
const (
	ErrAgentUnreachable       = "AGENT_UNREACHABLE"        // 连接 Agent 失败
	ErrAgentTimeout           = "AGENT_TIMEOUT"            // HTTP 分发超时
	ErrAgentInternal          = "AGENT_INTERNAL"           // Agent 返回 5xx
	ErrAgentProtocolViolation = "AGENT_PROTOCOL_VIOLATION" // 流不符合 SSE 协议
	ErrInvalidPayload         = "INVALID_PAYLOAD"          // 提交参数校验失败
	ErrToolTimeout            = "TOOL_TIMEOUT"             // 工具执行超时（不可重试，防副作用翻倍）
	ErrToolExecFailed         = "TOOL_EXEC_FAILED"         // 工具业务错误
	ErrSandboxViolation       = "SANDBOX_VIOLATION"        // 沙箱违规（安全事件）
	ErrRateLimited            = "RATE_LIMITED"             // 触发限速
	ErrCancelled              = "CANCELLED"                // 客户端取消
	ErrInternal               = "INTERNAL"                 // 控制面自身错误（兜底）
)

// ErrorClass 错误类别，决定控制面行为：
//
//	retryable -> Nack 重新入队（受 max_retries 约束）
//	permanent -> 直接置 failed
//	violation -> 置 failed + 审计告警（M5）
type ErrorClass string

const (
	ClassRetryable ErrorClass = "retryable"
	ClassPermanent ErrorClass = "permanent"
	ClassViolation ErrorClass = "violation"
)

// errorClasses 错误码 -> 类别映射，与 error-taxonomy.md 总表逐行对应。
var errorClasses = map[string]ErrorClass{
	ErrAgentUnreachable:       ClassRetryable,
	ErrAgentTimeout:           ClassRetryable,
	ErrAgentInternal:          ClassRetryable,
	ErrAgentProtocolViolation: ClassPermanent,
	ErrInvalidPayload:         ClassPermanent,
	ErrToolTimeout:            ClassPermanent,
	ErrToolExecFailed:         ClassPermanent,
	ErrSandboxViolation:       ClassViolation,
	ErrRateLimited:            ClassRetryable,
	ErrCancelled:              ClassPermanent,
	ErrInternal:               ClassRetryable,
}

// ClassOf 返回错误码的类别；未知码按 permanent 处理（保守策略，不会无限重试）。
func ClassOf(code string) ErrorClass {
	if c, ok := errorClasses[code]; ok {
		return c
	}
	return ClassPermanent
}

// IsRetryable 报告错误是否可重新入队。
func IsRetryable(code string) bool { return ClassOf(code) == ClassRetryable }
