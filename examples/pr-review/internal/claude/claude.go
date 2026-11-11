package claude

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/qisanfen666/agentflow/model"
	"github.com/qisanfen666/agentflow/runtime"
)

// TypeClaudeCode Runtime 类型名：AgentSpec.Runtime.Type 与之匹配即路由到本实现。
const TypeClaudeCode = "claude-code"

// 镜像约定（本地构建，不上传）：
//   agentflow-claude:latest        真实镜像：node + claude CLI（deploy 见 docker/Dockerfile.claude）
//   agentflow-claude-mock:latest   mock 镜像：回放录制的 stream-json（无需 API key 的冒烟基线）
const (
	DefaultImage     = "agentflow-claude:latest"
	DefaultMockImage = "agentflow-claude-mock:latest"
)

// DefaultImageEnv AgentSpec.Config 里覆盖镜像用的键（mock 切换不必重建 Agent：
// 管理员更新 spec 即可，走乐观锁与审计）。
const DefaultImageEnv = "CLAUDE_IMAGE"

// maxLineBytes 单行 stream-json 上限：tool_result 可能携带整个文件内容。
const maxLineBytes = 4 << 20 // 4MB

// Options Runtime 构造参数。
type Options struct {
	Image    string // 缺省 DefaultImage
	MemoryMB int64  // 容器内存上限（0 = 不限）
	NanoCPUs int64  // 容器 CPU 上限（0 = 不限）

	// LocalBin 非空 = 本地模式：跳过 docker，直接 exec 宿主机的 claude 可执行文件
	//（如 "claude" 或绝对路径），工作目录设为 payload.workspace。
	// 适用：本地开发验证 / 只读评审 Agent。代价：无文件系统隔离——Edit/Bash 类
	// 高危 Agent 不要用本地模式（白名单靠 claude 自律而非内核强制）。
	LocalBin string
}

func (o Options) withDefaults() Options {
	if o.Image == "" {
		o.Image = DefaultImage
	}
	return o
}

// Runtime 实现 agentflow 的 runtime.Runtime：一次性 docker 容器内跑 claude CLI，
// stream-json 经 stdout 逐行回流，转换为控制面 SSE 事件。
//
// 与内置 docker runtime 的差异：那边容器里是常驻 HTTP Agent（SSE 合同），
// 这边是 stdio 一次性进程（NDJSON 合同）——这正是 Runtime 扩展点存在的意义。
type Runtime struct {
	opts Options
}

// New 创建 claude-code Runtime。
func New(opts Options) *Runtime { return &Runtime{opts: opts.withDefaults()} }

func (r *Runtime) Type() string { return TypeClaudeCode }

// Health 探活（fail-fast，坏环境不接任务）：
//   - 容器模式：docker daemon 可达 + 镜像本地存在
//   - 本地模式：宿主机 claude 可执行
func (r *Runtime) Health(ctx context.Context, spec model.RuntimeSpec) error {
	if r.opts.LocalBin != "" {
		bin, args := localBinArgs(r.opts.LocalBin, []string{"--version"})
		if err := runShort(ctx, bin, args...); err != nil {
			return fmt.Errorf("local claude not runnable (%s): %w", r.opts.LocalBin, err)
		}
		return nil
	}
	if err := runShort(ctx, "docker", "version", "--format", "{{.Server.Version}}"); err != nil {
		return fmt.Errorf("docker daemon unreachable: %w", err)
	}
	img := imageOf(r.opts, spec)
	if err := runShort(ctx, "docker", "image", "inspect", img); err != nil {
		return fmt.Errorf("claude image %s not found: %w", img, err)
	}
	return nil
}

// imageOf 镜像优先级：spec.Image（Agent 版本化）> 环境覆盖 > 构造默认。
func imageOf(opts Options, spec model.RuntimeSpec) string {
	if spec.Image != "" {
		return spec.Image
	}
	return opts.Image
}

// Execute 跑一次性容器。事件合同与 runtime.Runtime 注释一致：错误以 error 事件交付。
func (r *Runtime) Execute(ctx context.Context, req runtime.Request) <-chan runtime.Event {
	ch := make(chan runtime.Event, 64)
	go func() {
		defer close(ch)
		r.stream(ctx, req, ch)
	}()
	return ch
}

func (r *Runtime) stream(ctx context.Context, req runtime.Request, ch chan<- runtime.Event) {
	prompt := buildPrompt(req.Payload, r.workspaceNote())
	cmd, err := r.buildCmd(ctx, req)
	if err != nil {
		fail(ch, model.ErrInvalidPayload, err)
		return
	}

	cmd.Stdin = bytes.NewReader([]byte(prompt))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fail(ch, model.ErrInternal, err)
		return
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		// 进程起不来属环境问题（可重试）：docker daemon / 本地 claude 缺失
		fail(ch, model.ErrAgentUnreachable, fmt.Errorf("%s: %w", r.exeName(), err))
		return
	}

	sawResult := pipeEvents(stdout, req.TaskID, ch)
	pipeErr := cmd.Wait()

	if ctx.Err() != nil {
		// 任务取消/超时：与内置 Runtime 语义一致，补 AGENT_TIMEOUT 错误事件
		//（ExecCommandContext 会 kill docker 客户端，--rm 容器随之拆除）
		fail(ch, model.ErrAgentTimeout, ctx.Err())
		return
	}
	if !sawResult {
		// 进程结束但没有 result 行：CLI 崩溃 / 环境坏（如 key 无效 / 镜像缺失）
		msg := fmt.Sprintf("claude exited without result: %v stderr=%s", pipeErr, truncate(stderr.String(), 300))
		code := model.ErrAgentInternal
		if pipeErr != nil && !isClaudeExitCode(pipeErr) {
			code = model.ErrAgentUnreachable // 容器/进程载体自身失败（daemon/镜像/挂载/可执行缺失）
		}
		fail(ch, code, errors.New(msg))
	}
}

// buildCmd 按模式构造执行体：本地直连 claude，或一次性 docker 容器。
func (r *Runtime) buildCmd(ctx context.Context, req runtime.Request) (*exec.Cmd, error) {
	if r.opts.LocalBin != "" {
		ws, _ := req.Payload["workspace"].(string)
		if ws == "" {
			return nil, fmt.Errorf("payload.workspace is required (PR checkout 的宿主目录)")
		}
		bin, args := localBinArgs(r.opts.LocalBin, claudeFlags(req))
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Dir = ws // claude 的工作目录 = PR 检出目录（Read/Grep 相对它解析）
		if len(req.Spec.Env) > 0 {
			cmd.Env = append(os.Environ(), envSlice(req.Spec.Env)...)
		}
		return cmd, nil
	}
	args, err := dockerArgs(r.opts, req)
	if err != nil {
		return nil, err
	}
	return exec.CommandContext(ctx, "docker", args...), nil
}

// localBinArgs 解析本地 claude 可执行。npm 全局安装在 Windows 上是 claude.cmd
// 批处理包装，CreateProcess 不能直接执行，须经 cmd.exe /c 转发（stdin/stdout 透传不受影响）。
func localBinArgs(bin string, args []string) (string, []string) {
	resolved, err := exec.LookPath(bin)
	if err == nil && (strings.EqualFold(filepath.Ext(resolved), ".cmd") || strings.EqualFold(filepath.Ext(resolved), ".bat")) {
		return "cmd.exe", append([]string{"/c", resolved}, args...)
	}
	return bin, args
}

// exeName 报错文案用。
func (r *Runtime) exeName() string {
	if r.opts.LocalBin != "" {
		return "local claude (" + r.opts.LocalBin + ")"
	}
	return "docker run"
}

// workspaceNote prompt 里的工作区提示按模式适配（docker 挂载点 vs 进程 cwd）。
func (r *Runtime) workspaceNote() string {
	if r.opts.LocalBin != "" {
		return "当前工作目录即 PR 检出目录，diff 见下方；需要上下文时用 Read/Grep 工具查看仓库文件。"
	}
	return "代码已在 /workspace 挂载，diff 见下方；需要上下文时用 Read/Grep 工具查看仓库文件。"
}

// pipeEvents 逐行读 NDJSON 转事件。返回是否见到 result 终局行。
// 结果事件里的 done/error 需要补上 task_id（claude 原生行不带）。
func pipeEvents(r io.Reader, taskID string, ch chan<- runtime.Event) bool {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
	conv := &converter{}
	sawResult := false
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		evs, isResult := conv.convertLine(line)
		if isResult {
			sawResult = true
		}
		for _, ev := range evs {
			switch ev.Type {
			case runtime.EventDone, runtime.EventError:
				ev.TaskID = taskID
			}
			ch <- ev
		}
	}
	if err := sc.Err(); err != nil {
		log.Printf("[claude] read stream: %v", err)
	}
	return sawResult
}

// dockerArgs 组装一次性容器命令。参数确定性（env 排序）保证可测试、可复现。
//
//	 docker run --rm -i
//		  -v <workspace>:/workspace[:ro|:rw]   # 只读默认；auto-fixer 类 Agent 声明 rw
//		  -e K=V ...                            # spec.Env（如 ANTHROPIC_API_KEY）
//		  [--memory m] [--cpus n]
//		  <image>
//		  claude -p --output-format stream-json --verbose
//		  [--allowedTools "Read Grep ..."] [--max-turns N]
//
// prompt 经 stdin 传入（不进 argv，杜绝 shell/参数注入面）。
func dockerArgs(opts Options, req runtime.Request) ([]string, error) {
	ws, _ := req.Payload["workspace"].(string)
	if ws == "" {
		return nil, fmt.Errorf("payload.workspace is required (PR checkout 的宿主目录)")
	}
	mode := ":ro"
	if ro, _ := req.AgentConfig["workspace_readonly"].(bool); ro == false {
		mode = ":rw" // 声明型放行：auto-fixer 等需要写回的 Agent（仍受一次性容器约束）
	}

	args := []string{"run", "--rm", "-i", "-v", toSlash(ws) + ":/workspace" + mode}
	for _, kv := range envSlice(req.Spec.Env) {
		args = append(args, "-e", kv)
	}
	if opts.MemoryMB > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dm", opts.MemoryMB))
	}
	if opts.NanoCPUs > 0 {
		args = append(args, "--cpus", fmt.Sprintf("%.2f", float64(opts.NanoCPUs)/1e9))
	}
	args = append(args, imageOf(opts, req.Spec))
	return append(args, claudeFlags(req)...), nil
}

// claudeFlags claude CLI 参数（docker / 本地两模式共用）。
// prompt 经 stdin 传入（不进 argv，杜绝 shell/参数注入面）。
func claudeFlags(req runtime.Request) []string {
	args := []string{"-p", "--output-format", "stream-json", "--verbose"}
	if tools, ok := req.AgentConfig["allowed_tools"].([]any); ok && len(tools) > 0 {
		whitelist := make([]string, 0, len(tools))
		for _, t := range tools {
			if s, ok := t.(string); ok {
				whitelist = append(whitelist, s)
			}
		}
		if len(whitelist) > 0 {
			args = append(args, "--allowedTools", joinArgs(whitelist))
		}
	}
	if n, ok := req.AgentConfig["max_turns"]; ok {
		if turns, err := toInt(n); err == nil && turns > 0 {
			args = append(args, "--max-turns", strconv.Itoa(turns))
		}
	}
	return args
}

// envSlice spec.Env -> 排序后的 "K=V" 列表（docker -e 与本地 cmd.Env 共用，确定性输出）。
func envSlice(env map[string]string) []string {
	out := make([]string, 0, len(env))
	for _, k := range slices.Sorted(maps.Keys(env)) {
		out = append(out, k+"="+env[k])
	}
	return out
}

// buildPrompt 由 payload 组装评审指令（业务知识留在此层，控制面不感知）。
// payload 约定：repo / pr / base / title / diff（由 CI 或本地脚本生成）；
// wsNote 为工作区提示（docker 挂载点 / 本地 cwd，见 Runtime.workspaceNote）。
func buildPrompt(payload map[string]any, wsNote string) string {
	get := func(k string) string { s, _ := payload[k].(string); return s }
	prompt := "你是资深代码评审员，评审以下 Pull Request。"
	if repo := get("repo"); repo != "" {
		prompt += "\n仓库: " + repo
	}
	if pr := get("pr"); pr != "" {
		prompt += "\nPR #" + pr
	}
	if base := get("base"); base != "" {
		prompt += "\n目标分支: " + base
	}
	if title := get("title"); title != "" {
		prompt += "\n标题: " + title
	}
	prompt += `

要求：
1. ` + wsNote + `
2. 按风险分级输出问题（blocker / major / minor），给出文件与行号依据。
3. 无问题的方面不要泛泛表扬，聚焦真实风险。
4. 用 Markdown 输出，最后给一行总结论：APPROVE / REQUEST_CHANGES。

--- DIFF ---
`
	return prompt + get("diff")
}

// ---------- 小工具 ----------

// runShort 跑短命令取退出码（Health 用）。
func runShort(ctx context.Context, name string, args ...string) error {
	return exec.CommandContext(ctx, name, args...).Run()
}

// isClaudeExitCode 区分"claude 进程退出"与"docker 本身失败"：
// ExitError = 容器跑起来了但退出非零（claude 层失败）；其余是 docker 层失败。
func isClaudeExitCode(err error) bool {
	var ee *exec.ExitError
	return errors.As(err, &ee)
}

func fail(ch chan<- runtime.Event, code string, err error) {
	ch <- runtime.Event{Type: runtime.EventError, Code: code, Message: truncate(err.Error(), 500)}
}

// toSlash Windows 路径转 docker 认的斜杠形式（E:\a\b -> E:/a/b）。
func toSlash(p string) string { return strings.ReplaceAll(p, "\\", "/") }

func joinArgs(parts []string) string { return strings.Join(parts, " ") }

func toInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case float64: // JSON 反序列化的数字
		return int(n), nil
	case string:
		return strconv.Atoi(n)
	}
	return 0, fmt.Errorf("not a number: %v", v)
}
