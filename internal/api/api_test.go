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
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/qisanfen666/agentflow/internal/dispatch"
	"github.com/qisanfen666/agentflow/internal/engine"
	"github.com/qisanfen666/agentflow/internal/observability"
	"github.com/qisanfen666/agentflow/internal/policy"
	"github.com/qisanfen666/agentflow/internal/registry"
	"github.com/qisanfen666/agentflow/model"
	"github.com/qisanfen666/agentflow/runtime"
	"github.com/qisanfen666/agentflow/storage"
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
	d := dispatch.New(agents, tasks, hub, observability.Telemetry{}, rt)
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

// ---------- Tools 端点 ----------

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

// TestSubmitCapturesTraceContext 提交侧捕获：tracing 启用时，
// 提交请求的 span 上下文以 traceparent 字符串存进任务记录。
func TestSubmitCapturesTraceContext(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	srv := setupAPI(t, &scriptedRuntime{})
	resp, body := postJSON(t, srv, "/api/v1/agents", map[string]any{
		"name": "tc", "type": "chat",
		"runtime": map[string]any{"type": "python-http", "host": "http://localhost:1"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create agent: %d %s", resp.StatusCode, body)
	}
	var agent model.AgentSpec
	json.Unmarshal([]byte(body), &agent)

	resp, body = postJSON(t, srv, "/api/v1/tasks", map[string]any{"agent_id": agent.ID, "payload": map[string]any{}})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit: %d %s", resp.StatusCode, body)
	}
	var task model.Task
	json.Unmarshal([]byte(body), &task)

	if task.TraceContext == "" {
		t.Fatal("submit should capture traceparent when tracing is enabled")
	}
	parts := strings.Split(task.TraceContext, "-")
	if len(parts) != 4 || len(parts[1]) != 32 || len(parts[2]) != 16 {
		t.Fatalf("malformed trace_context: %q", task.TraceContext)
	}
	// 捕获的是 API 根 span：spanID 应能在 recorder 的 api span 里找到
	found := false
	for _, span := range rec.Ended() {
		if span.SpanContext().SpanID().String() == parts[2] {
			found = true
		}
	}
	if !found {
		t.Fatalf("captured span id %s not found in api spans", parts[2])
	}
}

// ---------- 认证授权（M6） ----------

// TestAPIKeyAuthMatrix 401/403/200 三态：错 key 拒之门外，
// 越权方法 403，合法角色放行；/health 探活豁免。
func TestAPIKeyAuthMatrix(t *testing.T) {
	gin.SetMode(gin.TestMode)
	auth := NewAPIKeyAuth([]APIKeyEntry{
		{Key: "k-admin", Roles: []string{RoleAdmin}},
		{Key: "k-sub", Roles: []string{RoleSubmitter}},
		{Key: "k-read", Roles: []string{RoleReader}},
	})
	agents := storage.NewMemoryAgentStore()
	tasks := storage.NewMemoryTaskStore()
	hub := dispatch.NewHub()
	d := dispatch.New(agents, tasks, hub, observability.Telemetry{}, &scriptedRuntime{})
	srv := httptest.NewServer(NewRouter(Dependencies{
		Agents: agents, Tasks: tasks, Dispatcher: d, Hub: hub,
		Queue: engine.NewMemoryQueue(),
		Idem:  storage.NewMemoryIdemStore(),
		Tools: registry.New(storage.NewMemoryToolStore()),
		Auth:  auth,
	}))
	t.Cleanup(srv.Close)

	agent := mustCreateAgent(t, srv.URL, "k-admin")

	cases := []struct {
		name string
		key  string
		verb func() (*http.Response, string)
		want int
	}{
		{"无 key 提交被拒", "", func() (*http.Response, string) {
			return postAuth(t, srv.URL, "", "/api/v1/tasks", map[string]any{"agent_id": agent.ID, "payload": map[string]any{}})
		}, http.StatusUnauthorized},
		{"错 key 被拒", "k-wrong", func() (*http.Response, string) {
			return postAuth(t, srv.URL, "k-wrong", "/api/v1/tasks", map[string]any{"agent_id": agent.ID, "payload": map[string]any{}})
		}, http.StatusUnauthorized},
		{"reader 建 Agent 越权 403", "k-read", func() (*http.Response, string) {
			return postAuth(t, srv.URL, "k-read", "/api/v1/agents", map[string]any{
				"name": "x", "type": "chat",
				"runtime": map[string]any{"type": "python-http", "host": "http://localhost:1"},
			})
		}, http.StatusForbidden},
		{"reader 读 Agent 放行", "k-read", func() (*http.Response, string) {
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/agents", nil)
			req.Header.Set("X-API-Key", "k-read")
			resp, err := http.DefaultClient.Do(req)
			return resp, readBody(t, err, resp)
		}, http.StatusOK},
		{"submitter 提交放行", "k-sub", func() (*http.Response, string) {
			return postAuth(t, srv.URL, "k-sub", "/api/v1/tasks", map[string]any{"agent_id": agent.ID, "payload": map[string]any{}})
		}, http.StatusAccepted},
		{"health 探活豁免", "", func() (*http.Response, string) {
			resp, err := http.Get(srv.URL + "/health")
			return resp, readBody(t, err, resp)
		}, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := tc.verb()
			if resp == nil || resp.StatusCode != tc.want {
				t.Fatalf("want %d, got %v %s", tc.want, resp, body)
			}
		})
	}
}

func mustCreateAgent(t *testing.T, baseURL, key string) model.AgentSpec {
	t.Helper()
	resp, body := postAuth(t, baseURL, key, "/api/v1/agents", map[string]any{
		"name": "auth-demo", "type": "chat",
		"runtime": map[string]any{"type": "python-http", "host": "http://localhost:1"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("setup create agent: %d %s", resp.StatusCode, body)
	}
	var spec model.AgentSpec
	json.Unmarshal([]byte(body), &spec)
	return spec
}

func postAuth(t *testing.T, baseURL, key, path string, body any) (*http.Response, string) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp, readBody(t, nil, resp)
}

func readBody(t *testing.T, err error, resp *http.Response) string {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return string(raw)
}

// ---------- 治理（M6） ----------

// TestGovernanceRateLimited 限流守门：burst 打满后第 3 次提交 429 + Retry-After。
func TestGovernanceRateLimited(t *testing.T) {
	gin.SetMode(gin.TestMode)
	agents := storage.NewMemoryAgentStore()
	tasks := storage.NewMemoryTaskStore()
	hub := dispatch.NewHub()
	d := dispatch.New(agents, tasks, hub, observability.Telemetry{}, &scriptedRuntime{})
	srv := httptest.NewServer(NewRouter(Dependencies{
		Agents: agents, Tasks: tasks, Dispatcher: d, Hub: hub,
		Queue:  engine.NewMemoryQueue(),
		Idem:   storage.NewMemoryIdemStore(),
		Tools:  registry.New(storage.NewMemoryToolStore()),
		Policy: policy.Chain{policy.NewRateLimitRule(2)},
	}))
	t.Cleanup(srv.Close)

	resp, body := postJSON(t, srv, "/api/v1/agents", map[string]any{
		"name": "gov", "type": "chat",
		"runtime": map[string]any{"type": "python-http", "host": "http://localhost:1"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create agent: %d %s", resp.StatusCode, body)
	}
	var agent model.AgentSpec
	json.Unmarshal([]byte(body), &agent)

	submit := func() *http.Response {
		resp, _ := postJSON(t, srv, "/api/v1/tasks", map[string]any{"agent_id": agent.ID, "payload": map[string]any{}})
		return resp
	}
	if r := submit(); r.StatusCode != http.StatusAccepted {
		t.Fatalf("1st submit want 202, got %d", r.StatusCode)
	}
	if r := submit(); r.StatusCode != http.StatusAccepted {
		t.Fatalf("2nd submit want 202, got %d", r.StatusCode)
	}
	third := submit()
	if third.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("3rd submit want 429, got %d", third.StatusCode)
	}
	if third.Header.Get("Retry-After") == "" {
		t.Fatal("429 should carry Retry-After header")
	}
}

// ---------- 审批流（M6） ----------

// TestApprovalFlow 高危 Agent 全链路：提交即待审（不入队不执行）→ approve 放行
// → 入队执行至终态；重复审批 409。
func TestApprovalFlow(t *testing.T) {
	srv := setupAPI(t, &scriptedRuntime{events: []runtime.Event{{Type: runtime.EventDone, TaskID: "t"}}})

	resp, body := postJSON(t, srv, "/api/v1/agents", map[string]any{
		"name": "danger", "type": "chat", "require_approval": true,
		"runtime": map[string]any{"type": "python-http", "host": "http://localhost:1"},
	})
	if resp.StatusCode != http.StatusCreated || !strings.Contains(body, `"require_approval":true`) {
		t.Fatalf("create high-risk agent: %d %s", resp.StatusCode, body)
	}
	var agent model.AgentSpec
	json.Unmarshal([]byte(body), &agent)

	resp, body = postJSON(t, srv, "/api/v1/tasks", map[string]any{"agent_id": agent.ID, "payload": map[string]any{}})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit: %d %s", resp.StatusCode, body)
	}
	var task model.Task
	json.Unmarshal([]byte(body), &task)
	if task.Status != model.TaskPendingApproval {
		t.Fatalf("high-risk submit should be pending_approval, got %s", task.Status)
	}

	// 待审期间不执行：等 500ms 状态不变
	time.Sleep(500 * time.Millisecond)
	cur := getJSON(t, srv.URL+"/api/v1/tasks/"+task.ID)
	if cur.Status != model.TaskPendingApproval {
		t.Fatalf("task must not execute before approval, got %s", cur.Status)
	}

	// 审批放行 -> 入队执行到终态
	resp, _ = postJSON(t, srv, "/api/v1/tasks/"+task.ID+"/approve", map[string]any{})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("approve: %d", resp.StatusCode)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cur = getJSON(t, srv.URL+"/api/v1/tasks/"+task.ID)
		if cur.Status.Final() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if cur.Status != model.TaskSucceeded {
		t.Fatalf("after approve should execute to succeeded, got %s", cur.Status)
	}

	// 终态后重复审批 -> 409
	resp, _ = postJSON(t, srv, "/api/v1/tasks/"+task.ID+"/approve", map[string]any{})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("approve on final task want 409, got %d", resp.StatusCode)
	}
}

// TestApprovalReject 驳回：pending_approval -> cancelled 终态。
func TestApprovalReject(t *testing.T) {
	srv := setupAPI(t, &scriptedRuntime{})

	resp, body := postJSON(t, srv, "/api/v1/agents", map[string]any{
		"name": "danger2", "type": "chat", "require_approval": true,
		"runtime": map[string]any{"type": "python-http", "host": "http://localhost:1"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create agent: %d %s", resp.StatusCode, body)
	}
	var agent model.AgentSpec
	json.Unmarshal([]byte(body), &agent)

	resp, body = postJSON(t, srv, "/api/v1/tasks", map[string]any{"agent_id": agent.ID, "payload": map[string]any{}})
	var task model.Task
	json.Unmarshal([]byte(body), &task)

	resp, _ = postJSON(t, srv, "/api/v1/tasks/"+task.ID+"/reject", map[string]any{})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("reject: %d", resp.StatusCode)
	}
	if cur := getJSON(t, srv.URL+"/api/v1/tasks/"+task.ID); cur.Status != model.TaskCancelled {
		t.Fatalf("after reject want cancelled, got %s", cur.Status)
	}
}

// TestApproveRequiresAdmin 审批权仅限 admin。
func TestApproveRequiresAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	auth := NewAPIKeyAuth([]APIKeyEntry{{Key: "k-sub", Roles: []string{RoleSubmitter}}})
	agents := storage.NewMemoryAgentStore()
	tasks := storage.NewMemoryTaskStore()
	hub := dispatch.NewHub()
	d := dispatch.New(agents, tasks, hub, observability.Telemetry{}, &scriptedRuntime{})
	srv := httptest.NewServer(NewRouter(Dependencies{
		Agents: agents, Tasks: tasks, Dispatcher: d, Hub: hub,
		Queue: engine.NewMemoryQueue(), Idem: storage.NewMemoryIdemStore(),
		Tools: registry.New(storage.NewMemoryToolStore()), Auth: auth,
	}))
	t.Cleanup(srv.Close)

	resp, _ := postAuth(t, srv.URL, "k-sub", "/api/v1/tasks/t_x/approve", map[string]any{})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("submitter approve want 403, got %d", resp.StatusCode)
	}
}

func getJSON(t *testing.T, url string) model.Task {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var task model.Task
	json.NewDecoder(resp.Body).Decode(&task)
	return task
}

// ---------- 审计 ----------

// collectAudit 测试用审计收集器（与 dispatch 包的 bufAudit 同构，
// 跨包测试内小重复可接受）。
type collectAudit struct {
	mu  sync.Mutex
	evs []observability.AuditEvent
}

func (a *collectAudit) Record(_ context.Context, ev observability.AuditEvent) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.evs = append(a.evs, ev)
	return nil
}

func (a *collectAudit) actions() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.evs))
	for i, ev := range a.evs {
		out[i] = ev.Action
	}
	return out
}

// TestAuditTrailForAPI 全链路审计：API 层（created/submitted）与执行层
// （started/succeeded，由 worker goroutine 异步写入）在同一审计流里按序可见。
func TestAuditTrailForAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	audit := &collectAudit{}
	agents := storage.NewMemoryAgentStore()
	tasks := storage.NewMemoryTaskStore()
	hub := dispatch.NewHub()
	rt := &scriptedRuntime{events: []runtime.Event{{Type: runtime.EventDone, TaskID: "t"}}}
	d := dispatch.New(agents, tasks, hub, observability.Telemetry{Audit: audit}, rt)
	queue := engine.NewMemoryQueue()
	worker := engine.NewWorker(queue, d, tasks, engine.WorkerConfig{RetryBackoffBase: 10 * time.Millisecond})
	stop := worker.Start(context.Background())
	t.Cleanup(stop)
	srv := httptest.NewServer(NewRouter(Dependencies{
		Agents: agents, Tasks: tasks, Dispatcher: d, Hub: hub, Queue: queue,
		Idem:  storage.NewMemoryIdemStore(),
		Tools: registry.New(storage.NewMemoryToolStore()),
		Audit: audit,
	}))
	t.Cleanup(srv.Close)

	resp, body := postJSON(t, srv, "/api/v1/agents", map[string]any{
		"name": "a", "type": "chat",
		"runtime": map[string]any{"type": "python-http", "host": "http://localhost:1"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create agent: %d %s", resp.StatusCode, body)
	}
	var agent model.AgentSpec
	json.Unmarshal([]byte(body), &agent)

	resp, body = postJSON(t, srv, "/api/v1/tasks", map[string]any{"agent_id": agent.ID, "payload": map[string]any{}})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit task: %d %s", resp.StatusCode, body)
	}
	var task model.Task
	json.Unmarshal([]byte(body), &task)

	// 等 worker 侧审计落齐（异步）
	want := []string{
		observability.ActionAgentCreated,
		observability.ActionTaskSubmitted,
		observability.ActionTaskStarted,
		observability.ActionTaskSucceeded,
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := audit.actions(); len(got) >= len(want) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := audit.actions()
	if len(got) < len(want) {
		t.Fatalf("audit trail incomplete: %v, want prefix %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("audit trail order = %v, want %v", got, want)
		}
	}
}

// TestSessionIDAttribution 会话归因：提交带 session_id -> 任务回显，
// 且提交/终态审计事件的 detail 携带 session_id（高基数只进审计，不进指标）。
func TestSessionIDAttribution(t *testing.T) {
	gin.SetMode(gin.TestMode)
	audit := &collectAudit{}
	agents := storage.NewMemoryAgentStore()
	tasks := storage.NewMemoryTaskStore()
	hub := dispatch.NewHub()
	rt := &scriptedRuntime{events: []runtime.Event{{Type: runtime.EventDone, TaskID: "t"}}}
	d := dispatch.New(agents, tasks, hub, observability.Telemetry{Audit: audit}, rt)
	queue := engine.NewMemoryQueue()
	worker := engine.NewWorker(queue, d, tasks, engine.WorkerConfig{RetryBackoffBase: 10 * time.Millisecond})
	stop := worker.Start(context.Background())
	t.Cleanup(stop)
	srv := httptest.NewServer(NewRouter(Dependencies{
		Agents: agents, Tasks: tasks, Dispatcher: d, Hub: hub, Queue: queue,
		Idem: storage.NewMemoryIdemStore(), Tools: registry.New(storage.NewMemoryToolStore()),
		Audit: audit,
	}))
	t.Cleanup(srv.Close)

	resp, body := postJSON(t, srv, "/api/v1/agents", map[string]any{
		"name": "sess", "type": "chat",
		"runtime": map[string]any{"type": "python-http", "host": "http://localhost:1"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create agent: %d %s", resp.StatusCode, body)
	}
	var agent model.AgentSpec
	json.Unmarshal([]byte(body), &agent)

	resp, body = postJSON(t, srv, "/api/v1/tasks", map[string]any{
		"agent_id": agent.ID, "session_id": "s_001", "payload": map[string]any{},
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit: %d %s", resp.StatusCode, body)
	}
	var task model.Task
	json.Unmarshal([]byte(body), &task)
	if task.SessionID != "s_001" {
		t.Fatalf("session_id should roundtrip, got %q", task.SessionID)
	}

	// 等 worker 跑完，审计里 submitted 与 succeeded 都应带 session_id
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(audit.actions()) >= 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	audit.mu.Lock()
	defer audit.mu.Unlock()
	seen := map[string]bool{}
	for _, ev := range audit.evs {
		if ev.EntityID == task.ID && ev.Detail["session_id"] == "s_001" {
			seen[ev.Action] = true
		}
	}
	if !seen[observability.ActionTaskSubmitted] || !seen[observability.ActionTaskSucceeded] {
		t.Fatalf("submitted/succeeded audit should carry session_id, seen=%v events=%+v", seen, audit.evs)
	}
}
