package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/qisanfen666/agentflow/model"
)

// DefaultSandboxImage 沙箱 Agent 镜像（deploy/Dockerfile.agent-sandbox 构建，
// 内容 = examples/python-agent 打进 python:3.12-alpine）。
const DefaultSandboxImage = "agentflow-sandbox:latest"

// sandboxContainerPort 容器内 Agent 监听端口（agent.py 绑定 0.0.0.0:8081）。
const sandboxContainerPort = "8081"

// errHealthTimeout 沙箱健康等待超时的哨兵（映射 AGENT_TIMEOUT）。
var errHealthTimeout = errors.New("sandbox agent did not become healthy in time")

// SandboxOptions 沙箱资源限制，wiring 时映射自 agentflow.SandboxConfig。
// 注意：NetworkDisabled 本版不落地——SSE 传输依赖端口映射，禁网沙箱需要
// 换 exec/stdio 传输通道（M5+ 再议），这里明确不支持而不是静默忽略。
type SandboxOptions struct {
	ReadOnlyRootFS bool          // --read-only：根文件系统只读
	MemoryMB       int64         // --memory：内存上限（MB）
	NanoCPUs       int64         // --cpus：CPU 上限（1 核 = 1e9）
	HealthTimeout  time.Duration // 等容器内 Agent 就绪的上限，默认 30s
}

func (o SandboxOptions) withDefaults() SandboxOptions {
	if o.HealthTimeout <= 0 {
		o.HealthTimeout = 30 * time.Second
	}
	return o
}

// cmdRunner 抽象对外部命令的调用（单测注入 fake，不真跑 docker）。
type cmdRunner interface {
	run(ctx context.Context, name string, args ...string) (string, error)
}

type osRunner struct{}

func (osRunner) run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// Docker 是 docker 沙箱 Runtime：每个任务一个一次性容器。
// 生命周期：create（资源限制 + 随机端口绑定 127.0.0.1）→ start →
// 轮询健康 → 复用 httpDoStream 走 SSE 合同 → rm -f（无论成败必清理）。
// 操作方式为 shell 出 docker CLI（决策记录：SDK 依赖树过重，五条命令用不上）。
type Docker struct {
	runner cmdRunner
	client *http.Client
	opts   SandboxOptions
}

// NewDocker 创建沙箱 runtime。runner 为 nil 用真实 os/exec。
func NewDocker(opts SandboxOptions) *Docker {
	return &Docker{runner: osRunner{}, client: &http.Client{}, opts: opts.withDefaults()}
}

func (d *Docker) Type() string { return model.RuntimeDocker }

// Health 探活：daemon 可达 + 镜像在本地存在。
func (d *Docker) Health(ctx context.Context, spec model.RuntimeSpec) error {
	if _, err := d.runner.run(ctx, "docker", "version", "--format", "{{.Server.Version}}"); err != nil {
		return fmt.Errorf("docker daemon unreachable: %w", err)
	}
	if _, err := d.runner.run(ctx, "docker", "image", "inspect", d.image(spec)); err != nil {
		return fmt.Errorf("sandbox image not found: %w", err)
	}
	return nil
}

// Execute 执行任务：起一次性沙箱容器，就绪后走 SSE 内核，结束拆容器。
func (d *Docker) Execute(ctx context.Context, req Request) <-chan Event {
	ch := make(chan Event, 64)
	go func() {
		defer close(ch)

		addr, cleanup, err := d.startContainer(ctx, req)
		if err != nil {
			// 健康超时 -> AGENT_TIMEOUT；其余（daemon/镜像/端口）-> AGENT_UNREACHABLE。
			// 二者皆可重试，且都属环境问题而非任务问题。
			if errors.Is(err, errHealthTimeout) {
				fail(ch, model.ErrAgentTimeout, err)
			} else {
				fail(ch, model.ErrAgentUnreachable, err)
			}
			return
		}
		defer cleanup()

		body, err := json.Marshal(map[string]any{
			"task_id":      req.TaskID,
			"agent_config": req.AgentConfig,
			"payload":      req.Payload,
		})
		if err != nil {
			fail(ch, model.ErrInternal, err)
			return
		}
		httpDoStream(ctx, d.client, "http://"+addr+"/run/stream", body, ch)
	}()
	return ch
}

func (d *Docker) image(spec model.RuntimeSpec) string {
	if spec.Image != "" {
		return spec.Image
	}
	return DefaultSandboxImage
}

// startContainer 创建并启动沙箱容器，返回容器映射地址与清理函数。
// 端口用 -p 127.0.0.1::8081：宿主随机端口、只绑回环（不暴露局域网）。
func (d *Docker) startContainer(ctx context.Context, req Request) (addr string, cleanup func(), err error) {
	name := "agentflow-sbx-" + req.TaskID

	args := []string{
		"create", "--name", name,
		"-p", "127.0.0.1::" + sandboxContainerPort,
	}
	if d.opts.ReadOnlyRootFS {
		args = append(args, "--read-only")
	}
	if d.opts.MemoryMB > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dm", d.opts.MemoryMB))
	}
	if d.opts.NanoCPUs > 0 {
		args = append(args, "--cpus", fmt.Sprintf("%.2f", float64(d.opts.NanoCPUs)/1e9))
	}
	// 排序保证参数确定性（可测试、可复现）
	for _, k := range slices.Sorted(maps.Keys(req.Spec.Env)) {
		args = append(args, "-e", k+"="+req.Spec.Env[k])
	}
	args = append(args, d.image(req.Spec))

	if _, err := d.runner.run(ctx, "docker", args...); err != nil {
		return "", nil, fmt.Errorf("docker create: %w", err)
	}
	cleanup = func() {
		// 用独立 ctx：任务 ctx 可能已取消，但容器必须拆掉（防泄漏）
		if _, err := d.runner.run(context.Background(), "docker", "rm", "-f", name); err != nil {
			log.Printf("[docker] cleanup %s: %v", name, err)
		}
	}

	if _, err := d.runner.run(ctx, "docker", "start", name); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("docker start: %w", err)
	}

	portOut, err := d.runner.run(ctx, "docker", "port", name, sandboxContainerPort)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("docker port: %w", err)
	}
	addr = parseDockerPort(portOut)
	if addr == "" {
		cleanup()
		return "", nil, fmt.Errorf("no published port for container %s (output %q)", name, portOut)
	}

	if err := d.waitHealthy(ctx, addr); err != nil {
		cleanup()
		return "", nil, err
	}
	return addr, cleanup, nil
}

// waitHealthy 轮询容器内 Agent 的 /health。语义与 python-http 一致：
// 任何 HTTP 应答即视为可达（网络通了、进程活着），不要求业务语义。
func (d *Docker) waitHealthy(ctx context.Context, addr string) error {
	deadline := time.Now().Add(d.opts.HealthTimeout)
	probe := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		resp, err := probe.Get("http://" + addr + "/health")
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errHealthTimeout
}

// parseDockerPort 解析 `docker port` 输出（形如 "127.0.0.1:55123\n[::1]:55123"），
// 取第一行的 host:port。
func parseDockerPort(out string) string {
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, ":") {
			return line
		}
	}
	return ""
}
