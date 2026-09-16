// Package runtime 是公开扩展点①：Agent 执行环境抽象。
// 用户实现 Runtime 接口即可接入自定义执行环境（K8s Job、Firecracker VM、远程集群等），
// 内置实现：python-http（HTTP 直连）、docker（M3 沙箱）。
package runtime

import (
	"context"
	"encoding/json"

	"agentflow/model"
)

// SSE 事件类型常量，对应 docs/protocol/sse.md v1。
const (
	EventToken = "token"
	EventEvent = "event"
	EventUsage = "usage"
	EventError = "error"
	EventDone  = "done"
)

// Event 是 SSE 协议 5 类事件的统一 Go 表示。
// 单结构体覆盖全部类型，序列化结果与线上 JSON 一致（omitempty 收敛）。
type Event struct {
	Type string `json:"type"`

	// token
	Content string `json:"content,omitempty"`

	// event（thought | tool_call | tool_result）
	Name string          `json:"event,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`

	// usage（多次出现由消费方累加）
	PromptTokens     int64  `json:"prompt_tokens,omitempty"`
	CompletionTokens int64  `json:"completion_tokens,omitempty"`
	Model            string `json:"model,omitempty"`

	// error / done
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	TaskID  string `json:"task_id,omitempty"`
}

// Request 是控制面下发给 Runtime 的执行请求。
type Request struct {
	TaskID      string            // 控制面任务 ID
	Spec        model.RuntimeSpec // 该 Agent 锁定版本的执行配置（host/image/env）
	AgentConfig map[string]any    // AgentSpec.Config 原样透传
	Payload     map[string]any    // 任务输入
}

// Runtime 是执行环境抽象。实现契约：
//
//   - Execute 不阻塞：立即返回事件通道，流结束或出错后关闭通道
//   - 错误以事件交付：出错误时先发一个 Type=EventError 的事件再关闭通道（错误即事件，
//     不用二段式返回值，消费方只需处理一条流）
//   - 必须尊重 ctx：取消/超时后停止拉取上游并关闭通道
//   - Type() 的返回值用于路由：与 AgentSpec.Runtime.Type 匹配
type Runtime interface {
	Type() string
	Execute(ctx context.Context, req Request) <-chan Event
	Health(ctx context.Context, spec model.RuntimeSpec) error
}
