package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"agentflow/internal/dispatch"
	"agentflow/internal/engine"
	"agentflow/internal/registry"
	"agentflow/model"
	"agentflow/runtime"
	"agentflow/storage"
)

// scriptedRuntime 按脚本回放事件 / 挂起等取消的假 Runtime。
// 与 dispatch 包的 fake 分属两个包，测试内小重复可接受（不为此抽公共 testutil）。
type scriptedRuntime struct {
	events   []runtime.Event
	blocking bool
}

func (f *scriptedRuntime) Type() string                                        { return model.RuntimePythonHTTP }
func (f *scriptedRuntime) Health(_ context.Context, _ model.RuntimeSpec) error { return nil }

func (f *scriptedRuntime) Execute(ctx context.Context, _ runtime.Request) <-chan runtime.Event {
	ch := make(chan runtime.Event, 8)
	go func() {
		defer close(ch)
		if f.blocking {
			<-ctx.Done()
			return
		}
		for _, ev := range f.events {
			ch <- ev
		}
	}()
	return ch
}

func setupAPI(t *testing.T, rt runtime.Runtime) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	agents := storage.NewMemoryAgentStore()
	tasks := storage.NewMemoryTaskStore()
	hub := dispatch.NewHub()
	d := dispatch.New(agents, tasks, hub, rt)
	queue := engine.NewMemoryQueue()
	worker := engine.NewWorker(queue, d, tasks, engine.WorkerConfig{RetryBackoffBase: 10 * time.Millisecond})
	stop := worker.Start(context.Background())
	t.Cleanup(stop)
	srv := httptest.NewServer(NewRouter(Dependencies{
		Agents: agents, Tasks: tasks, Dispatcher: d, Hub: hub, Queue: queue,
		Idem:  storage.NewMemoryIdemStore(),
		Tools: registry.New(storage.NewMemoryToolStore()),
	}))
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, srv *httptest.Server, method, path string, body any, headers map[string]string) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, srv.URL+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

// waitStatus 轮询任务状态直到期望值（上限 2s）。
func waitStatus(t *testing.T, srv *httptest.Server, taskID string, want model.TaskStatus) model.Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var task model.Task
		resp := do(t, srv, http.MethodGet, "/api/v1/tasks/"+taskID, nil, nil)
		if resp.StatusCode == http.StatusOK {
			decode(t, resp, &task)
			if task.Status == want {
				return task
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s never reached %s", taskID, want)
	return model.Task{}
}

func TestAgentCRUDViaHTTP(t *testing.T) {
	srv := setupAPI(t, &scriptedRuntime{})
	agentBody := map[string]any{
		"name":    "a1",
		"type":    "chat",
		"runtime": map[string]any{"type": "python-http", "host": "http://agent:8081"},
	}

	// 创建 -> 201
	var created model.AgentSpec
	resp := do(t, srv, http.MethodPost, "/api/v1/agents", agentBody, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create = %d", resp.StatusCode)
	}
	decode(t, resp, &created)
	if created.ID == "" || created.Version != 1 {
		t.Fatalf("create should assign id/v1, got %+v", created)
	}

	// 更新（正确 base）-> v2
	agentBody["name"] = "a1-v2"
	upd := map[string]any{"name": "a1-v2", "type": "chat",
		"runtime": agentBody["runtime"], "base_version": 1}
	var updated model.AgentSpec
	resp = do(t, srv, http.MethodPut, "/api/v1/agents/"+created.ID, upd, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("update = %d", resp.StatusCode)
	}
	decode(t, resp, &updated)
	if updated.Version != 2 {
		t.Fatalf("update should yield v2, got %d", updated.Version)
	}

	// 过期 base -> 409
	resp = do(t, srv, http.MethodPut, "/api/v1/agents/"+created.ID, upd, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale base should 409, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 版本历史 2 条
	var vs []model.AgentSpec
	resp = do(t, srv, http.MethodGet, "/api/v1/agents/"+created.ID+"/versions", nil, nil)
	decode(t, resp, &vs)
	if len(vs) != 2 {
		t.Fatalf("versions = %d, want 2", len(vs))
	}

	// 删除 -> 204，再 Get -> 404
	resp = do(t, srv, http.MethodDelete, "/api/v1/agents/"+created.ID, nil, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete = %d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = do(t, srv, http.MethodGet, "/api/v1/agents/"+created.ID, nil, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get after delete = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestSubmitAndStream(t *testing.T) {
	rt := &scriptedRuntime{events: []runtime.Event{
		{Type: runtime.EventToken, Content: "你好"},
		{Type: runtime.EventToken, Content: "，控制面"},
		{Type: runtime.EventUsage, PromptTokens: 5, CompletionTokens: 7},
		{Type: runtime.EventDone},
	}}
	srv := setupAPI(t, rt)

	// 建 Agent
	var agent model.AgentSpec
	resp := do(t, srv, http.MethodPost, "/api/v1/agents", map[string]any{
		"name": "a", "type": "chat",
		"runtime": map[string]any{"type": "python-http", "host": "http://x"},
	}, nil)
	decode(t, resp, &agent)

	// 提交任务 -> 202
	var task model.Task
	resp = do(t, srv, http.MethodPost, "/api/v1/tasks", map[string]any{
		"agent_id": agent.ID, "payload": map[string]any{"q": "hi"},
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit = %d", resp.StatusCode)
	}
	decode(t, resp, &task)
	if task.AgentVersion != 1 {
		t.Fatalf("submitted task should lock version 1, got %d", task.AgentVersion)
	}

	// SSE 流：拿到全部事件 + [DONE]
	resp = do(t, srv, http.MethodGet, "/api/v1/tasks/"+task.ID+"/stream", nil, nil)
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	var datas []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		d := strings.TrimPrefix(line, "data: ")
		if d == "[DONE]" {
			break
		}
		datas = append(datas, d)
	}
	resp.Body.Close()

	if len(datas) != 4 {
		t.Fatalf("want 4 events before [DONE], got %d: %v", len(datas), datas)
	}
	var first, last runtime.Event
	json.Unmarshal([]byte(datas[0]), &first)
	json.Unmarshal([]byte(datas[3]), &last)
	if first.Type != runtime.EventToken || first.Content != "你好" {
		t.Errorf("first event = %+v", first)
	}
	if last.Type != runtime.EventDone {
		t.Errorf("last event = %+v", last)
	}

	// 终态 succeeded + usage 累加
	final := waitStatus(t, srv, task.ID, model.TaskSucceeded)
	if final.Usage == nil || final.Usage.PromptTokens != 5 || final.Usage.CompletionTokens != 7 {
		t.Fatalf("usage = %+v", final.Usage)
	}
}

func TestIdempotencyKey(t *testing.T) {
	srv := setupAPI(t, &scriptedRuntime{events: []runtime.Event{{Type: runtime.EventDone}}})
	var agent model.AgentSpec
	decode(t, do(t, srv, http.MethodPost, "/api/v1/agents", map[string]any{
		"name": "a", "type": "chat",
		"runtime": map[string]any{"type": "python-http", "host": "http://x"},
	}, nil), &agent)

	hdr := map[string]string{"Idempotency-Key": "k-1"}
	body := map[string]any{"agent_id": agent.ID, "payload": map[string]any{}}
	var t1, t2 model.Task
	decode(t, do(t, srv, http.MethodPost, "/api/v1/tasks", body, hdr), &t1)
	decode(t, do(t, srv, http.MethodPost, "/api/v1/tasks", body, hdr), &t2)
	if t1.ID != t2.ID {
		t.Fatalf("same idempotency key must return same task, got %s vs %s", t1.ID, t2.ID)
	}
}

func TestCancelViaHTTP(t *testing.T) {
	srv := setupAPI(t, &scriptedRuntime{blocking: true})
	var agent model.AgentSpec
	decode(t, do(t, srv, http.MethodPost, "/api/v1/agents", map[string]any{
		"name": "a", "type": "chat",
		"runtime": map[string]any{"type": "python-http", "host": "http://x"},
	}, nil), &agent)

	var task model.Task
	decode(t, do(t, srv, http.MethodPost, "/api/v1/tasks", map[string]any{
		"agent_id": agent.ID, "payload": map[string]any{},
	}, nil), &task)

	waitStatus(t, srv, task.ID, model.TaskRunning)

	resp := do(t, srv, http.MethodDelete, "/api/v1/tasks/"+task.ID, nil, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel = %d", resp.StatusCode)
	}
	var cancelled model.Task
	decode(t, resp, &cancelled)
	if cancelled.Status != model.TaskCancelled {
		t.Fatalf("status = %s", cancelled.Status)
	}

	// 终态后再取消 -> 409
	resp = do(t, srv, http.MethodDelete, "/api/v1/tasks/"+task.ID, nil, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("cancel on final = %d, want 409", resp.StatusCode)
	}
	resp.Body.Close()
}

// ---------- Tools 端点（M3） ----------

func toolPayload() map[string]any {
	return map[string]any{
		"name":        "search_docs",
		"description": "检索文档",
		"parameters": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string"},
			},
			"required": []any{"query"},
		},
		"timeout_sec": 45,
	}
}

func postJSON(t *testing.T, srv *httptest.Server, path string, body any) (*http.Response, string) {
	data, _ := json.Marshal(body)
	resp, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(raw)
}

func TestToolLifecycleViaHTTP(t *testing.T) {
	srv := setupAPI(t, &scriptedRuntime{}) // 工具端点不触发执行，哑 runtime 占位

	// 注册成功：字段回显 + 默认值不覆盖显式值
	resp, body := postJSON(t, srv, "/api/v1/tools", toolPayload())
	if resp.StatusCode != http.StatusCreated || !strings.Contains(body, `"search_docs"`) || !strings.Contains(body, `"timeout_sec":45`) {
		t.Fatalf("register: %d %s", resp.StatusCode, body)
	}
	var created model.ToolDef
	json.Unmarshal([]byte(body), &created)

	// 名字重复 -> 409
	if resp, _ := postJSON(t, srv, "/api/v1/tools", toolPayload()); resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate want 409, got %d", resp.StatusCode)
	}

	// schema 非法 -> 400
	bad := toolPayload()
	bad["parameters"] = map[string]any{"type": "nonsense"}
	if resp, _ := postJSON(t, srv, "/api/v1/tools", bad); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad schema want 400, got %d", resp.StatusCode)
	}

	// 列表 / 单查 / 404
	if resp, body = get(srv, "/api/v1/tools"); resp.StatusCode != http.StatusOK || !strings.Contains(body, created.ID) {
		t.Fatalf("list: %d %s", resp.StatusCode, body)
	}
	if resp, _ = get(srv, "/api/v1/tools/"+created.ID); resp.StatusCode != http.StatusOK {
		t.Fatalf("get: %d", resp.StatusCode)
	}
	if resp, _ = get(srv, "/api/v1/tools/tool_none"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get missing want 404, got %d", resp.StatusCode)
	}

	// MCP 导出：单个 + 全量
	if resp, body = get(srv, "/api/v1/tools/"+created.ID+"/mcp"); resp.StatusCode != http.StatusOK || !strings.Contains(body, `"type":"function"`) {
		t.Fatalf("mcp single: %d %s", resp.StatusCode, body)
	}
	if resp, body = get(srv, "/api/v1/tools/mcp"); resp.StatusCode != http.StatusOK || !strings.Contains(body, `"search_docs"`) {
		t.Fatalf("mcp list: %d %s", resp.StatusCode, body)
	}
}

func get(srv *httptest.Server, path string) (*http.Response, string) {
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		return nil, ""
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(raw)
}
