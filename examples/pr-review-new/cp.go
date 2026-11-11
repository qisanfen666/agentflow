package main

// cp.go —— 控制面 loopback 客户端：中台与控制面之间只走公开 HTTP API
//（X-API-Key 认证），证明嵌入形态无需触达库内部。
//
// 角色验证：keyForRole 按当前视角取 key——越权请求由控制面权限矩阵
// 真实拒绝（403），GUI 原样展示，作为治理三层防线的演示依据。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type cpClient struct {
	base      string // http://127.0.0.1:PORT/agentflow
	adminKey  string
	submitKey string
	readerKey string
	hc        *http.Client
}

func newCPClient(base string, cfg envConfig) *cpClient {
	return &cpClient{
		base: base, adminKey: cfg.adminKey, submitKey: cfg.submitKey, readerKey: cfg.readerKey,
		hc: &http.Client{Timeout: 10 * time.Second},
	}
}

// keyForRole 视角 -> 凭据。角色合法性由控制面裁决，中台不做本地豁免。
func (c *cpClient) keyForRole(role string) string {
	switch role {
	case "reader":
		return c.readerKey
	case "submitter":
		return c.submitKey
	default:
		return c.adminKey
	}
}

// do 调控制面 API。返回状态码；out 非 nil 时解响应体。
// 4xx/5xx 返回 err（含响应体摘要），调用方按 code 决定透传给 GUI 的形态。
func (c *cpClient) do(ctx context.Context, method, path, key string, body any, out any, extra map[string]string) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("%s %s -> %d: %s", method, path, resp.StatusCode, truncateStr(string(raw), 300))
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode %s: %w", path, err)
		}
	}
	return resp.StatusCode, nil
}

// stream 打开任务 SSE 流（历史回放 + 增量），供直播代理与回写消费。
// 返回的 body 由调用方关闭；非 2xx 返回错误。
func (c *cpClient) stream(ctx context.Context, taskID string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.base+"/api/v1/tasks/"+taskID+"/stream", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", c.readerKey)
	hc := &http.Client{} // 不设超时：SSE 长连接
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, fmt.Errorf("stream %s -> %d", taskID, resp.StatusCode)
	}
	return resp.Body, nil
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
