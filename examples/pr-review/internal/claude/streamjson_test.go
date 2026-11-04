package claude

import (
	"strings"
	"testing"

	"github.com/qisanfen666/agentflow/model"
	"github.com/qisanfen666/agentflow/runtime"
)

// 录制形态的 claude stream-json 片段（结构对齐真实 CLI 输出，内容为 demo 仓库场景）。
const fixtureSession = `{"type":"system","subtype":"init","session_id":"s_01","tools":["Read","Grep","Glob"],"model":"claude-sonnet-4-5"}
{"type":"assistant","message":{"model":"claude-sonnet-4-5","content":[{"type":"text","text":"我先读一下改动的文件。"}]},"session_id":"s_01"}
{"type":"assistant","message":{"model":"claude-sonnet-4-5","content":[{"type":"tool_use","id":"toolu_01","name":"Read","input":{"file_path":"/workspace/orders.go"}}]},"session_id":"s_01"}
{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_01","content":"func getByUser(u string) { q := \"SELECT * FROM orders WHERE user = '\" + u + \"'\" }","is_error":false}]},"session_id":"s_01"}
{"type":"assistant","message":{"model":"claude-sonnet-4-5","content":[{"type":"text","text":"发现 SQL 拼接注入风险。"}]},"session_id":"s_01"}
{"type":"result","subtype":"success","is_error":false,"num_turns":2,"result":"## PR Review\n- blocker: SQL 注入","session_id":"s_01","usage":{"input_tokens":1200,"output_tokens":300},"total_cost_usd":0.01}
`

func TestConvertLineSuccess(t *testing.T) {
	conv := &converter{}
	var got []runtime.Event
	sawResult := false
	for _, line := range strings.Split(fixtureSession, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		evs, isResult := conv.convertLine([]byte(line))
		if isResult {
			sawResult = true
		}
		got = append(got, evs...)
	}

	if !sawResult {
		t.Fatal("result 行未识别")
	}
	// 事件序列断言：init thought + token + tool_call + tool_result + token + usage + done
	if len(got) != 7 {
		t.Fatalf("事件数 = %d, want 7: %+v", len(got), got)
	}
	if got[0].Name != "thought" {
		t.Errorf("首事件应为 init thought, got %+v", got[0])
	}
	if got[1].Type != runtime.EventToken || got[1].Content != "我先读一下改动的文件。" {
		t.Errorf("token 事件不符: %+v", got[1])
	}
	if got[2].Name != "tool_call" || !strings.Contains(string(got[2].Data), `"tool":"Read"`) {
		t.Errorf("tool_call 事件不符: %+v", got[2])
	}
	if got[3].Name != "tool_result" || !strings.Contains(string(got[3].Data), "SELECT") {
		t.Errorf("tool_result 事件不符: %+v", got[3])
	}
	last := got[len(got)-1]
	if last.Type != runtime.EventDone {
		t.Errorf("末事件应为 done, got %+v", last)
	}
	u := got[5]
	if u.Type != runtime.EventUsage || u.PromptTokens != 1200 || u.CompletionTokens != 300 || u.Model != "claude-sonnet-4-5" {
		t.Errorf("usage 事件不符: %+v", u)
	}
}

func TestConvertLineErrorResult(t *testing.T) {
	line := `{"type":"result","subtype":"error_max_turns","is_error":true,"result":"exceeded","usage":{"input_tokens":10,"output_tokens":5}}`
	evs, isResult := (&converter{}).convertLine([]byte(line))
	if !isResult || len(evs) != 2 {
		t.Fatalf("error result 应产出 usage+error: %+v", evs)
	}
	if evs[1].Type != runtime.EventError || evs[1].Code != "TOOL_EXEC_FAILED" {
		t.Errorf("错误码映射不符: %+v", evs[1])
	}
}

func TestConvertLineToleratesNoise(t *testing.T) {
	for _, line := range []string{``, `not json`, `{"type":"stream_event"}`} {
		if evs, isResult := (&converter{}).convertLine([]byte(line)); len(evs) != 0 || isResult {
			t.Errorf("噪行应被容忍: %q -> %+v", line, evs)
		}
	}
}

func TestPipeEventsAttachesTaskID(t *testing.T) {
	ch := make(chan runtime.Event, 8)
	go func() {
		defer close(ch)
		if !pipeEvents(strings.NewReader(fixtureSession), "t_9f3k2", ch) {
			t.Error("pipeEvents 未识别 result")
		}
	}()
	var done runtime.Event
	for ev := range ch {
		if ev.Type == runtime.EventDone {
			done = ev
		}
	}
	if done.TaskID != "t_9f3k2" {
		t.Fatalf("done 应带 task_id: %+v", done)
	}
}

func TestDockerArgs(t *testing.T) {
	req := runtime.Request{
		TaskID: "t_1",
		Spec: model.RuntimeSpec{
			Type:  TypeClaudeCode,
			Image: "agentflow-claude-mock:latest",
			Env:   map[string]string{"ANTHROPIC_API_KEY": "sk-x", "B": "2"},
		},
		AgentConfig: map[string]any{
			"allowed_tools":       []any{"Read", "Grep", "Glob"},
			"max_turns":           float64(8),
			"workspace_readonly":  true,
		},
		Payload: map[string]any{"workspace": `E:\demo\repo`},
	}
	args, err := dockerArgs(Options{}, req)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"run --rm -i",
		"-v E:/demo/repo:/workspace:ro",
		"-e ANTHROPIC_API_KEY=sk-x -e B=2", // env 排序确定性
		"agentflow-claude-mock:latest",
		"claude -p --output-format stream-json --verbose",
		"--allowedTools Read Grep Glob",
		"--max-turns 8",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("args 缺少 %q:\n%s", want, joined)
		}
	}

	// rw 放行 + 缺 workspace 报错
	req.AgentConfig["workspace_readonly"] = false
	if args, _ = dockerArgs(Options{}, req); !strings.Contains(strings.Join(args, " "), ":rw") {
		t.Error("workspace_readonly=false 应挂 :rw")
	}
	delete(req.Payload, "workspace")
	if _, err := dockerArgs(Options{}, req); err == nil {
		t.Error("缺 workspace 应报错")
	}
}

func TestBuildPromptContainsDiff(t *testing.T) {
	p := buildPrompt(map[string]any{
		"repo": "svc-orders", "pr": "101", "base": "main",
		"title": "feat: orders query", "diff": "+SELECT * FROM",
	})
	for _, want := range []string{"svc-orders", "#101", "main", "feat: orders query", "+SELECT * FROM", "APPROVE"} {
		if !strings.Contains(p, want) {
			t.Errorf("prompt 缺少 %q", want)
		}
	}
}
