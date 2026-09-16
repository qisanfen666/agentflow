package runtime

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qisanfen666/agentflow/model"
)

// fakeRunner 记录全部命令调用；hook 可按需返回特定输出/错误。
type fakeRunner struct {
	mu      sync.Mutex
	calls   [][]string
	hook    func(args []string) (string, error)
	portOut string
}

func (f *fakeRunner) run(_ context.Context, name string, args ...string) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string{name}, args...))
	f.mu.Unlock()
	if f.hook != nil {
		return f.hook(args)
	}
	switch args[0] {
	case "port":
		return f.portOut, nil
	default:
		return "", nil
	}
}

func (f *fakeRunner) commands() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.calls))
	copy(out, f.calls)
	return out
}

// flatten 便于断言 "create/start/port/rm -f" 这样的命令序列。
func flatten(calls [][]string) string {
	var verbs []string
	for _, c := range calls {
		if len(c) >= 2 {
			verbs = append(verbs, c[1])
		}
	}
	return strings.Join(verbs, "/")
}

func findCall(calls [][]string, verb string) []string {
	for _, c := range calls {
		if len(c) >= 2 && c[1] == verb {
			return c
		}
	}
	return nil
}

// newSandboxAgent 起一个扮演"容器内 Agent"的 httptest 服务：/health + /run/stream。
func newSandboxAgent(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		io.WriteString(w, "data: {\"type\":\"token\",\"content\":\"沙箱OK\"}\n\n")
		io.WriteString(w, "data: {\"type\":\"done\"}\n\n")
		io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func lastErr(evs []Event) *Event {
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Type == EventError {
			return &evs[i]
		}
	}
	return nil
}

func sbxRequest() Request {
	return Request{
		TaskID: "t_sbx",
		Spec: model.RuntimeSpec{
			Type:  model.RuntimeDocker,
			Image: "my-agent:1.0",
			Env:   map[string]string{"B": "2", "A": "1"},
		},
		Payload: map[string]any{"q": "hi"},
	}
}

// TestDockerHappyPath 全链路：create(带限制) -> start -> port -> 健康即通 -> SSE -> rm -f。
func TestDockerHappyPath(t *testing.T) {
	agent := newSandboxAgent(t)
	fr := &fakeRunner{portOut: agent.Listener.Addr().String() + "\n"}
	d := &Docker{
		runner: fr,
		client: &http.Client{},
		opts:   SandboxOptions{ReadOnlyRootFS: true, MemoryMB: 128, NanoCPUs: 5e8}.withDefaults(),
	}

	evs := collect(d.Execute(context.Background(), sbxRequest()))

	if lastErr(evs) != nil {
		t.Fatalf("unexpected error event: %+v", lastErr(evs))
	}
	if evs[0].Type != EventToken || evs[0].Content != "沙箱OK" {
		t.Fatalf("first event = %+v, want token 沙箱OK", evs[0])
	}

	calls := fr.commands()
	if got := flatten(calls); got != "create/start/port/rm" {
		t.Fatalf("command sequence = %q, want create/start/port/rm", got)
	}

	create := findCall(calls, "create")
	joined := strings.Join(create, " ")
	for _, want := range []string{
		"-p 127.0.0.1::8081", // 随机回环端口
		"--read-only",        // 只读根文件系统
		"--memory 128m",      // 内存限制
		"--cpus 0.50",        // CPU 限制（0.5 核）
		"-e A=1 -e B=2",      // env 按键排序注入
		"my-agent:1.0",       // 镜像取自 spec
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("create args missing %q in: %s", want, joined)
		}
	}

	rm := findCall(calls, "rm")
	if rm[2] != "-f" || rm[3] != "agentflow-sbx-t_sbx" {
		t.Errorf("rm call = %v", rm)
	}
}

// TestDockerCreateFails create 失败 -> AGENT_UNREACHABLE，且不得 rm（还没创建出容器）。
func TestDockerCreateFails(t *testing.T) {
	fr := &fakeRunner{hook: func(args []string) (string, error) {
		if args[0] == "create" {
			return "", errFake
		}
		return "", nil
	}}
	d := &Docker{runner: fr, client: &http.Client{}, opts: SandboxOptions{}.withDefaults()}

	evs := collect(d.Execute(context.Background(), sbxRequest()))

	le := lastErr(evs)
	if le == nil || le.Code != model.ErrAgentUnreachable {
		t.Fatalf("want AGENT_UNREACHABLE, got %+v", evs)
	}
	if got := flatten(fr.commands()); got != "create" {
		t.Fatalf("commands = %q, want only create", got)
	}
}

var errFake = &fakeError{}

type fakeError struct{}

func (*fakeError) Error() string { return "fake failure" }

// TestDockerHealthTimeout 容器起了但 Agent 一直不就绪 -> AGENT_TIMEOUT + 清理容器。
func TestDockerHealthTimeout(t *testing.T) {
	fr := &fakeRunner{portOut: "127.0.0.1:1\n"} // 无人监听的地址
	d := &Docker{runner: fr, client: &http.Client{}, opts: SandboxOptions{HealthTimeout: 300 * time.Millisecond}.withDefaults()}

	start := time.Now()
	evs := collect(d.Execute(context.Background(), sbxRequest()))

	le := lastErr(evs)
	if le == nil || le.Code != model.ErrAgentTimeout {
		t.Fatalf("want AGENT_TIMEOUT, got %+v", evs)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Fatalf("returned too early (%v), health wait not respected", elapsed)
	}
	if got := flatten(fr.commands()); got != "create/start/port/rm" {
		t.Fatalf("commands = %q, want create/start/port/rm (启动过的容器必须清理)", got)
	}
}

// TestDockerDefaultImage spec 未指定镜像 -> 用默认沙箱镜像。
func TestDockerDefaultImage(t *testing.T) {
	agent := newSandboxAgent(t)
	fr := &fakeRunner{portOut: agent.Listener.Addr().String() + "\n"}
	d := &Docker{runner: fr, client: &http.Client{}, opts: SandboxOptions{}.withDefaults()}

	req := sbxRequest()
	req.Spec.Image = ""
	collect(d.Execute(context.Background(), req))

	create := findCall(fr.commands(), "create")
	if got := create[len(create)-1]; got != DefaultSandboxImage {
		t.Fatalf("image = %q, want default %q", got, DefaultSandboxImage)
	}
}

func TestDockerHealthCheck(t *testing.T) {
	d := &Docker{runner: &fakeRunner{}, client: &http.Client{}, opts: SandboxOptions{}.withDefaults()}

	if err := d.Health(context.Background(), model.RuntimeSpec{}); err != nil {
		t.Fatalf("healthy env should pass: %v", err)
	}

	fr := &fakeRunner{hook: func(args []string) (string, error) {
		if len(args) >= 2 && args[1] == "inspect" { // docker image inspect ...
			return "", errFake // 镜像不存在
		}
		return "27.5.1", nil
	}}
	d2 := &Docker{runner: fr, client: &http.Client{}, opts: SandboxOptions{}.withDefaults()}
	if err := d2.Health(context.Background(), model.RuntimeSpec{}); err == nil {
		t.Fatal("missing image should fail health")
	}
}

func TestParseDockerPort(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1:55123\n[::1]:55123\n": "127.0.0.1:55123",
		"0.0.0.0:32768\n":                "0.0.0.0:32768",
		"\n":                             "",
		"":                               "",
	}
	for in, want := range cases {
		if got := parseDockerPort(in); got != want {
			t.Errorf("parseDockerPort(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestDockerIntegration 真机集成：需要 Docker Desktop + agentflow-sandbox 镜像
// （R3 冒烟时构建）。设 AGENTFLOW_DOCKER_IT=1 且镜像存在才运行，否则 skip。
func TestDockerIntegration(t *testing.T) {
	if os.Getenv("AGENTFLOW_DOCKER_IT") != "1" {
		t.Skip("设 AGENTFLOW_DOCKER_IT=1 且已构建 agentflow-sandbox 镜像后运行")
	}
	d := NewDocker(SandboxOptions{MemoryMB: 256, NanoCPUs: 1e9})

	spec := model.RuntimeSpec{Type: model.RuntimeDocker}
	if err := d.Health(context.Background(), spec); err != nil {
		t.Skipf("镜像未构建: %v", err)
	}

	evs := collect(d.Execute(context.Background(), Request{
		TaskID:  "t_it",
		Spec:    spec,
		Payload: map[string]any{"q": "integration"},
	}))
	le := lastErr(evs)
	if le != nil {
		t.Fatalf("integration error: %+v", le)
	}
	// agent.py 先发 thought 事件再发 token：断言流里有 token 且无错误即可
	hasToken := false
	for _, ev := range evs {
		if ev.Type == EventToken {
			hasToken = true
		}
	}
	if !hasToken {
		t.Fatalf("no token in stream: %+v", evs)
	}
}
