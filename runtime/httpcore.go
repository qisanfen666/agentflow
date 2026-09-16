package runtime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"agentflow/model"
)

// 本文件是 SSE 客户端内核：与 Agent 建立一次 POST /run/stream 连接，
// 把 SSE 流解析成 Event 通道，并把传输层异常映射为错误分类学的码。
// python-http（直连）与 docker（沙箱内连）两个 Runtime 共用它，
// 差别只在"URL 怎么来"和"执行环境生命周期"。

// doneSentinel SSE 流结束标记（沿用 OpenAI 流式惯例，见协议文档第 6 节）。
const doneSentinel = "[DONE]"

// httpDoStream 对 url 发起 SSE 请求并解析事件流写入 ch。
// 错误一律以 EventError 投递（"错误即事件"契约），随后由调用方关通道。
func httpDoStream(ctx context.Context, client *http.Client, url string, reqBody []byte, ch chan<- Event) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		fail(ch, model.ErrAgentUnreachable, err)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := client.Do(httpReq)
	if err != nil {
		// 区分"超时/取消"与"连不上"：前者是 AGENT_TIMEOUT（可重试语义不同）
		if ctx.Err() != nil {
			fail(ch, model.ErrAgentTimeout, ctx.Err())
		} else {
			fail(ch, model.ErrAgentUnreachable, err)
		}
		return
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 500:
		fail(ch, model.ErrAgentInternal, fmt.Errorf("agent returned %s", resp.Status))
		return
	case resp.StatusCode >= 400:
		fail(ch, model.ErrAgentProtocolViolation, fmt.Errorf("agent returned %s", resp.Status))
		return
	case resp.StatusCode != http.StatusOK:
		fail(ch, model.ErrAgentInternal, fmt.Errorf("unexpected status %s", resp.Status))
		return
	}

	if scanEvents(resp, ch) {
		return
	}
	// 走到这里说明流正常 EOF 但没有 [DONE]：违反协议
	fail(ch, model.ErrAgentProtocolViolation, errors.New("stream ended without [DONE]"))
}

// scanEvents 逐行解析 SSE 流并向 ch 转发。
// 返回 true 表示终止事件已投递（[DONE] / error 事件 / 已 fail），
// 调用方不得再补发任何事件；返回 false 仅表示"干净 EOF 但缺 [DONE]"，由调用方补协议违约。
func scanEvents(resp *http.Response, ch chan<- Event) bool {
	scanner := bufio.NewScanner(resp.Body)
	// 单行上限 1MB：默认 64KB 装不下大 event（如长 tool_result）
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "" || strings.HasPrefix(line, ":"):
			// 事件边界行 / 心跳注释（": ping"），直接忽略
			continue
		case strings.HasPrefix(line, "data:"):
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == doneSentinel {
				return true
			}
			var ev Event
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				fail(ch, model.ErrAgentProtocolViolation, fmt.Errorf("invalid event json %q: %v", data, err))
				return true
			}
			if ev.Type == "" {
				fail(ch, model.ErrAgentProtocolViolation, fmt.Errorf("event missing type: %q", data))
				return true
			}
			ch <- ev
			// 错误即终点：后续若有事件属于协议违约，但 v1 选择直接停读（简单且安全）
			if ev.Type == EventError {
				return true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if resp.Request != nil && resp.Request.Context().Err() != nil {
			fail(ch, model.ErrAgentTimeout, err)
		} else {
			fail(ch, model.ErrAgentInternal, err)
		}
		return true
	}
	// scanner 正常结束但没读到 [DONE]
	return false
}

func fail(ch chan<- Event, code string, err error) {
	ch <- Event{Type: EventError, Code: code, Message: err.Error()}
}
