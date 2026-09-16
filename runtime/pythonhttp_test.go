package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/qisanfen666/agentflow/model"
)

// collect 消费事件通道直到关闭。
func collect(ch <-chan Event) []Event {
	var out []Event
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func testReq(host string) Request {
	return Request{
		TaskID:  "t_test",
		Spec:    model.RuntimeSpec{Type: model.RuntimePythonHTTP, Host: host},
		Payload: map[string]any{"q": "hello"},
	}
}

// sse 逐条写出事件行并 flush。
func sse(w http.ResponseWriter, lines ...string) {
	flusher := w.(http.Flusher)
	for _, l := range lines {
		fmt.Fprintf(w, "data: %s\n\n", l)
		flusher.Flush()
	}
}

func TestHappyPath(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/run/stream" || r.Method != http.MethodPost {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		if body["task_id"] != "t_test" {
			t.Errorf("task_id not delivered, got %v", body["task_id"])
		}

		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, ": ping\n\n") // 心跳注释：必须被忽略
		sse(w,
			`{"type":"event","event":"thought","data":{"text":"先想想"}}`,
			`{"type":"token","content":"Hel"}`,
			`{"type":"token","content":"lo"}`,
			`{"type":"usage","prompt_tokens":10,"completion_tokens":5,"model":"gpt-4o"}`,
			`{"type":"done","task_id":"t_test"}`,
			`[DONE]`,
		)
	}))
	defer ts.Close()

	evs := collect(NewPythonHTTP().Execute(context.Background(), testReq(ts.URL)))

	if len(evs) != 5 {
		t.Fatalf("want 5 events (heartbeat ignored), got %d: %+v", len(evs), evs)
	}
	if evs[0].Type != EventEvent || evs[0].Name != "thought" {
		t.Errorf("event[0] should be thought, got %+v", evs[0])
	}
	if evs[1].Content != "Hel" || evs[2].Content != "lo" {
		t.Errorf("token order broken: %+v %+v", evs[1], evs[2])
	}
	if evs[3].PromptTokens != 10 || evs[3].CompletionTokens != 5 {
		t.Errorf("usage fields lost: %+v", evs[3])
	}
	if evs[4].Type != EventDone {
		t.Errorf("last event should be done, got %+v", evs[4])
	}
}

func TestErrorEventStopsStream(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sse(w,
			`{"type":"token","content":"partial"}`,
			`{"type":"error","code":"TOOL_TIMEOUT","message":"search timed out"}`,
			// error 后再发 token 属协议违约，runtime 应停读（不会收到这条）
			`{"type":"token","content":"should-not-appear"}`,
			`[DONE]`,
		)
	}))
	defer ts.Close()

	evs := collect(NewPythonHTTP().Execute(context.Background(), testReq(ts.URL)))

	if len(evs) != 2 {
		t.Fatalf("stream must stop at error event, got %d events: %+v", len(evs), evs)
	}
	if evs[1].Type != EventError || evs[1].Code != model.ErrToolTimeout {
		t.Fatalf("agent error should pass through, got %+v", evs[1])
	}
}

func TestMissingDoneSentinel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sse(w, `{"type":"token","content":"x"}`) // 干净 EOF 但没有 [DONE]
	}))
	defer ts.Close()

	evs := collect(NewPythonHTTP().Execute(context.Background(), testReq(ts.URL)))

	if len(evs) != 2 || evs[1].Type != EventError || evs[1].Code != model.ErrAgentProtocolViolation {
		t.Fatalf("missing [DONE] should yield protocol violation, got %+v", evs)
	}
}

func TestBadEventJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sse(w, `{oops`)
	}))
	defer ts.Close()

	evs := collect(NewPythonHTTP().Execute(context.Background(), testReq(ts.URL)))

	if len(evs) != 1 || evs[0].Code != model.ErrAgentProtocolViolation {
		t.Fatalf("bad json should yield single violation, got %+v", evs)
	}
}

func TestHTTPStatusMapping(t *testing.T) {
	cases := []struct {
		status int
		code   string
	}{
		{http.StatusInternalServerError, model.ErrAgentInternal},
		{http.StatusNotFound, model.ErrAgentProtocolViolation},
	}
	for _, c := range cases {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(c.status)
		}))
		evs := collect(NewPythonHTTP().Execute(context.Background(), testReq(ts.URL)))
		ts.Close()
		if len(evs) != 1 || evs[0].Code != c.code {
			t.Errorf("status %d should map to %s, got %+v", c.status, c.code, evs)
		}
	}
}

func TestUnreachable(t *testing.T) {
	// 端口 1 几乎必然连接拒绝
	evs := collect(NewPythonHTTP().Execute(context.Background(), testReq("http://127.0.0.1:1")))
	if len(evs) != 1 || evs[0].Code != model.ErrAgentUnreachable {
		t.Fatalf("refused connection should be unreachable, got %+v", evs)
	}
}

func TestTaskTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond) // 迟迟不响应
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	evs := collect(NewPythonHTTP().Execute(ctx, testReq(ts.URL)))
	if len(evs) != 1 || evs[0].Code != model.ErrAgentTimeout {
		t.Fatalf("deadline should yield timeout, got %+v", evs)
	}
}

func TestHealth(t *testing.T) {
	rt := NewPythonHTTP()

	// 任何 HTTP 应答（含 404）= 可达
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	if err := rt.Health(context.Background(), model.RuntimeSpec{Host: ts.URL}); err != nil {
		t.Errorf("reachable agent should be healthy, got %v", err)
	}
	ts.Close()

	if err := rt.Health(context.Background(), model.RuntimeSpec{Host: "http://127.0.0.1:1"}); err == nil {
		t.Error("unreachable agent should fail health check")
	}
}
