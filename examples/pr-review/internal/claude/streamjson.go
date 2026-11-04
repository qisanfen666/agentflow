// Package claude 是自定义 Runtime 示范（公开扩展点①）：把 Claude Code CLI 的
// stream-json NDJSON 输出适配为 agentflow 的 SSE 事件流。
//
// 本包属于 PR Review 场景 demo（examples/pr-review），不进库——claude 的调用细节、
// prompt 组装、工具白名单语义都是业务知识，控制面只认 runtime.Runtime 合同。
package claude

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/qisanfen666/agentflow/runtime"
)

// ---------- claude stream-json 线上格式（只声明用到的字段） ----------

type claudeEvent struct {
	Type      string         `json:"type"` // system | assistant | user | result
	Subtype   string         `json:"subtype"`
	Message   *claudeMessage `json:"message"`
	Model     string         `json:"model"`
	SessionID string         `json:"session_id"`
	Tools     []string       `json:"tools"`
	Result    string         `json:"result"` // 仅 result 事件：最终文本
	Usage     *claudeUsage   `json:"usage"`
	IsError   bool           `json:"is_error"`
}

type claudeMessage struct {
	Model   string        `json:"model"`
	Content []claudeBlock `json:"content"`
}

type claudeBlock struct {
	Type      string          `json:"type"` // text | tool_use | tool_result
	Text      string          `json:"text"`
	Name      string          `json:"name"` // tool_use：工具名
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"` // tool_result：string 或 blocks 数组
	IsError   bool            `json:"is_error"`
}

type claudeUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// converter 有状态的行转换器：model 从 assistant 行携带（claude 的 result 行
// 不带 model，而 usage 归因需要）。
type converter struct {
	model string
}

// convertLine 把一行 claude stream-json 转为 0..N 个 agentflow 事件。
// 未知 type / 无法解析的行返回空切片（前向兼容：协议演进不炸流）。
// 返回的第二个值表示该行是否终结了流（result 事件）。
func (c *converter) convertLine(line []byte) (evs []runtime.Event, isResult bool) {
	var ce claudeEvent
	if err := json.Unmarshal(line, &ce); err != nil {
		return nil, false // 容忍噪行（如 CLI 混入的警告输出）
	}
	switch ce.Type {
	case "system":
		return convertInit(ce), false
	case "assistant":
		if ce.Message != nil && ce.Message.Model != "" {
			c.model = ce.Message.Model
		}
		return convertAssistant(ce), false
	case "user":
		return convertToolResult(ce), false
	case "result":
		if c.model != "" {
			ce.Model = c.model
		}
		return convertResult(ce), true
	}
	return nil, false
}

// convertInit system/init：会话就绪（模型/工具集），作 thought 里程碑透出。
func convertInit(ce claudeEvent) []runtime.Event {
	data, _ := json.Marshal(map[string]any{
		"phase":      "init",
		"model":      ce.Model,
		"session_id": ce.SessionID,
		"tools":      ce.Tools,
	})
	return []runtime.Event{{Type: runtime.EventEvent, Name: "thought", Data: data}}
}

// convertAssistant assistant 消息：text 块 → token；tool_use 块 → tool_call。
func convertAssistant(ce claudeEvent) []runtime.Event {
	if ce.Message == nil {
		return nil
	}
	var evs []runtime.Event
	for _, b := range ce.Message.Content {
		switch b.Type {
		case "text":
			if b.Text != "" {
				evs = append(evs, runtime.Event{Type: runtime.EventToken, Content: b.Text})
			}
		case "tool_use":
			evs = append(evs, runtime.Event{Type: runtime.EventEvent, Name: "tool_call",
				Data: marshalMap("tool", b.Name, "args", rawOrEmpty(b.Input))})
		}
	}
	return evs
}

// convertToolResult user 消息里的 tool_result 块 → tool_result 事件（内容原样透传）。
func convertToolResult(ce claudeEvent) []runtime.Event {
	if ce.Message == nil {
		return nil
	}
	var evs []runtime.Event
	for _, b := range ce.Message.Content {
		if b.Type != "tool_result" {
			continue
		}
		evs = append(evs, runtime.Event{Type: runtime.EventEvent, Name: "tool_result",
			Data: marshalMap("tool_use_id", b.ToolUseID, "content", rawToString(b.Content), "is_error", b.IsError)})
	}
	return evs
}

// convertResult 终局：usage + done（成功）或 usage + error（失败）。
// usage 先于终态发出——失败前已消耗的 token 也要归集进预算/审计。
func convertResult(ce claudeEvent) []runtime.Event {
	var evs []runtime.Event
	if ce.Usage != nil {
		evs = append(evs, runtime.Event{Type: runtime.EventUsage,
			PromptTokens: ce.Usage.InputTokens, CompletionTokens: ce.Usage.OutputTokens,
			Model: ce.Model})
	}
	if ce.IsError || ce.Subtype != "success" {
		code := mapResultError(ce.Subtype)
		evs = append(evs, runtime.Event{Type: runtime.EventError, Code: code,
			Message: fmt.Sprintf("claude %s: %s", ce.Subtype, truncate(ce.Result, 500))})
		return evs
	}
	evs = append(evs, runtime.Event{Type: runtime.EventDone})
	return evs
}

// mapResultError claude result 子类型 -> agentflow 错误码：
// 执行中断属环境/上游问题（可重试）；轮数耗尽/拒绝属任务语义（不可重试）。
func mapResultError(subtype string) string {
	switch subtype {
	case "error_max_turns":
		return "TOOL_EXEC_FAILED"
	case "error_during_execution":
		return "AGENT_INTERNAL"
	default:
		return "AGENT_INTERNAL"
	}
}

// ---------- 小工具 ----------

func rawOrEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}
	return raw
}

// rawToString tool_result 的 content 可能是 string 或 blocks 数组：尝试按 string
// 解，失败则原样保留 JSON。
func rawToString(raw json.RawMessage) any {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return json.RawMessage(raw)
}

// marshalMap 有序组装小 JSON（key 固定顺序，输出确定、可测试）。
func marshalMap(kv ...any) json.RawMessage {
	var sb strings.Builder
	sb.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			sb.WriteByte(',')
		}
		k, _ := kv[i].(string)
		v, _ := json.Marshal(kv[i+1])
		kb, _ := json.Marshal(k)
		sb.Write(kb)
		sb.WriteByte(':')
		sb.Write(v)
	}
	sb.WriteByte('}')
	return json.RawMessage(sb.String())
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
