package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"agentflow/model"
)

// PythonHTTP 是 python-http Runtime：HTTP 直连外部 Agent。
// 合同：POST {host}/run/stream → SSE 事件流，详见 docs/protocol/sse.md。
// 一个实例可服务多个 Agent：host 从每次请求的 Spec.Host 取。
// SSE 解析/错误映射复用 httpcore.go 的内核。
type PythonHTTP struct {
	client *http.Client
}

// NewPythonHTTP 创建 runtime。
// client 故意不设全局 Timeout：流式响应会被任何全局超时掐断，
// 生命周期完全由 ctx 控制（调用方负责设置任务级 deadline）。
func NewPythonHTTP() *PythonHTTP {
	return &PythonHTTP{client: &http.Client{}}
}

func (p *PythonHTTP) Type() string { return model.RuntimePythonHTTP }

// Health 探活：任何 HTTP 应答（含 404）即视为可达，语义是"网络可达"而非"业务健康"。
func (p *PythonHTTP) Health(ctx context.Context, spec model.RuntimeSpec) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(spec.Host, "/")+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// Execute 执行任务，立即返回事件通道；流结束/出错后通道关闭（错误即事件契约）。
func (p *PythonHTTP) Execute(ctx context.Context, req Request) <-chan Event {
	ch := make(chan Event, 64)
	go func() {
		defer close(ch)
		body, err := json.Marshal(map[string]any{
			"task_id":      req.TaskID,
			"agent_config": req.AgentConfig,
			"payload":      req.Payload,
		})
		if err != nil {
			fail(ch, model.ErrInternal, err)
			return
		}
		url := strings.TrimSuffix(req.Spec.Host, "/") + "/run/stream"
		httpDoStream(ctx, p.client, url, body, ch)
	}()
	return ch
}
