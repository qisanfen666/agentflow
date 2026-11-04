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

```powershell
# 构建真实镜像（node + claude CLI）
cd examples/pr-review/docker
docker build -f Dockerfile.claude -t agentflow-claude:latest .

# 起中台：切镜像 + 注入 key（key 只进 AgentSpec.Env -> 容器，不落代码/日志）
cd ../../..
$env:CLAUDE_IMAGE = "agentflow-claude:latest"
$env:ANTHROPIC_API_KEY = "sk-ant-..."
go run ./examples/pr-review
```

GUI 新建 PR（svc-orders / svc-payments）即对 `testdata/` 里的真实 git 仓库与
真实 git diff 发起评审；claude 在一次性容器内只读挂载仓库，产出分级问题清单。

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
| CLAUDE_IMAGE | agentflow-claude-mock:latest | mock / 真实镜像切换 |
| ANTHROPIC_API_KEY | — | 真实模式的 key（注入 AgentSpec.Env） |
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
