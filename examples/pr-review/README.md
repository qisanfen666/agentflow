# PR Review 中台（agentflow 场景 demo）

用 agentflow 控制面搭一个"企业 PR Review 中台"：CI 触发 Claude Code 容器化评审、
token 级直播到 PR 页、生产仓库任务级审批、成本按 PR 归因、审计留痕。
故事线见仓库根 `tmp/pr-review-story.md`（本地草稿）。

## 快速开始（mock 模式，无需 API key）

```powershell
# 0) 生成测试仓库与 PR diff（幂等；也是 CI 侧命令参考）
powershell -File examples/pr-review/scripts/make-testdata.ps1

# 1) 构建 mock claude 镜像（回放录制的 stream-json 会话）
cd examples/pr-review/docker
docker build -f Dockerfile.claude-mock -t agentflow-claude-mock:latest .

# 2) 起中台（仓库根目录执行）
cd ../../..
go run ./examples/pr-review
# GUI: http://localhost:8090  —— 左侧"新建 PR"即模拟 CI 触发
```

## 真实模式（真实 PR 检验）

三种执行模式按环境变量切换：

### 模式 A：本地直连（最快上手，无需 docker/镜像）

宿主机已装 claude CLI（`npm i -g @anthropic-ai/claude-code`）即可；认证用你本机的
`claude` 登录态（或宿主 env 的 `ANTHROPIC_API_KEY`）。

```powershell
$env:CLAUDE_LOCAL = "1"          # 可选 CLAUDE_BIN 指定可执行路径（默认 claude）
go run ./examples/pr-review      # GUI 头部芯片显示 LOCAL
```

注意：本地模式**无文件系统隔离**——只读评审 Agent（Read/Grep/Glob 白名单）适用；
auto-fixer（Edit/Bash）请勿本地跑。

### 模式 B：一次性容器（强隔离，生产形态）

```powershell
# 构建真实镜像（node + claude CLI）
cd examples/pr-review/docker
docker build -f Dockerfile.claude -t agentflow-claude:latest .

# 国内网络：若基础镜像拉取失败（镜像加速器不可达），先经可达源拉取再构建：
#   docker pull docker.1ms.run/library/node:20-slim
#   docker tag docker.1ms.run/library/node:20-slim node:20-slim

# 起中台：切镜像 + 注入 key（key 只进 AgentSpec.Env -> 容器，不落代码/日志）
cd ../../..
$env:CLAUDE_IMAGE = "agentflow-claude:latest"
$env:ANTHROPIC_API_KEY = "sk-ant-..."
go run ./examples/pr-review
```

### 模式 C：mock（默认）

无需 key 与真实镜像，回放录制的 stream-json 会话（GUI 与链路完全同构）。

GUI 新建 PR（svc-orders / svc-payments）即对 `testdata/` 里的真实 git 仓库与
真实 git diff 发起评审；claude 在容器内只读挂载仓库（模式 B）或以 PR 目录为
工作目录（模式 A），产出分级问题清单。

## 故事五阶段 -> 实现落点

| 阶段 | 实现 |
|---|---|
| 0 管理员初始化 | [bootstrap.go](bootstrap.go)：幂等注册 pr-reviewer / pr-reviewer-prod（审批）/ auto-fixer（审批+可写） |
| 1 CI 触发 | [watcher.go](watcher.go) `submitPR`：Idempotency-Key=PR 号、session_id 按 PR 归因、治理守门（限流+预算） |
| 2 执行+直播 | `internal/claude` Runtime：一次性容器 + stream-json 转事件；[api.go](api.go) `streamEvents` SSE 代理（历史回放+增量） |
| 3 审批闸门 | 控制面原生 `pending_approval`；GUI approve/reject（admin key 只在中台进程内） |
| 4 结果回写 | `writeback`：终态拉全量事件流聚合，结论（APPROVE/REQUEST_CHANGES）回写 PR |

## CI 接入（预留）

- 契约：`POST /webhook/pr`，体 `{repo, pr, title, workspace, diff}`；`workspace` 为
  中台可读的 PR 检出目录（自托管 runner 同机），`diff` 为 `git diff base..head` 产物。
  响应 202 + `{task_id, task_status, session_id}`。重复调用幂等（返回原任务）。
- 示例工作流：[cicd/github-actions-pr-review.yml.example](cicd/github-actions-pr-review.yml.example)
- 结果消费：轮询 `GET /api/board`（conclusion 字段）或订阅任务 SSE。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| PORT | 8090 | 中台监听端口 |
| CLAUDE_LOCAL | — | 非空 = 本地直连宿主 claude（模式 A） |
| CLAUDE_BIN | claude | 模式 A 的可执行名/路径 |
| CLAUDE_IMAGE | agentflow-claude-mock:latest | mock / 真实镜像切换（模式 B/C） |
| ANTHROPIC_API_KEY | — | claude 认证：官方 key 或中转 key（跳过 `claude login` 的标准方式）；模式 A 若本机已登录可省 |
| ANTHROPIC_BASE_URL | — | Anthropic 兼容网关地址（用中转/自建网关时设置，官方可省） |
| ANTHROPIC_AUTH_TOKEN | — | Bearer 型网关令牌（少数中转需要，与 API_KEY 二选一按网关要求） |
| STORAGE / REDIS_ADDR | memory | 控制面存储（redis = 重启不丢任务） |
| RATE_LIMIT_PER_MIN / DAILY_TOKEN_BUDGET | 60 / 5000000 | 治理护栏 |
| AUDIT_PATH | tmp/pr-audit.jsonl | 审计 JSONL |
| DEMO_ROOT | . | testdata 解析基点（默认仓库根运行） |

## 边界与解耦

- 本 demo 只 import agentflow 公开面（`agentflow` / `runtime` / `model`）+ 自带
  `internal/claude`；不触碰库 `internal/**`
- claude 调用细节（prompt、工具白名单语义）全在 demo 侧；控制面只认
  `runtime.Runtime` 合同（`WithRuntime` 注入，见库 `agent.go`）
- 工具白名单经 `AgentSpec.Config` 透传 -> `--allowedTools`；风险决策在管理端 spec
  （乐观锁变更、审计留痕），调用方不能自选
