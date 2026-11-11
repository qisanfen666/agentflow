# PR Review 中台（pr-review-new：裸 agent 生产化验证）

用 agentflow 控制面搭一个"企业 PR Review 中台"，**执行面是一个裸 agent**：
零依赖 Python 进程，只有最基础的 LLM 调用 + `read_file`/`write_file` 工具循环。
它实现一个 SSE 端点（`POST /run/stream`）即接入，控制面赋予它排队、审批、
直播、回写、预算、审计全部生产化能力——这就是本项目要验证的命题：

> **裸 agent 能力 + 控制面 = 生产级服务。**

故事线见 [pr-review-story.md](pr-review-story.md)。

## 快速开始（mock 模式，零外部依赖，不用 docker）

两个终端（仓库根目录执行）：

```powershell
# 终端 1：启动裸 agent（默认 mock 模式：无 key 时内置假 LLM）
python examples/pr-review-new/agent/agent.py

# 终端 2：启动中台（嵌入 agentflow 控制面 + GUI）
go run ./examples/pr-review-new
# GUI: http://localhost:8090
```

## 配置文件（推荐）

[config.json](config.json) 是中台与裸 agent **共用**的统一配置（已被 gitignore，
模板见 [config.example.json](config.example.json)）。优先级：**环境变量 > 文件 > 默认值**；
路径可用 `CONFIG` 环境变量改指。三个段：

| 段 | 消费方 | 字段 |
|---|---|---|
| `llm` | 裸 agent（终端 1） | `base_url` / `api_key` / `model` / `timeout_sec`。填上 key 重启 agent 即真实 LLM；留空自动 mock |
| `github` | 中台（真实仓库 + CI） | `token`（PAT）；`repos[]` 映射 `{owner, repo, prod}`（留空 = 本地 testdata）；`poll_sec`（CI 轮询间隔，0=关）；`post_comments`（结论回写 PR 评论）；`branch_prefix`（只自动评审该前缀分支，防误评审真实 PR） |
| `midway` | 中台（终端 2） | `port` / `agent_addr` / 三角色 key / `storage`+`redis_addr` / `rate_limit_per_min` / `daily_token_budget` / `audit_path` |

对应环境变量（同名覆盖文件）：`OPENAI_BASE_URL` `OPENAI_API_KEY` `OPENAI_MODEL`
`LLM_TIMEOUT_SEC`；`GITHUB_TOKEN` `GITHUB_API_BASE` `CI_POLL_SEC` `CI_POST_COMMENTS`
`CI_BRANCH_PREFIX`；`PORT` `AGENT_ADDR` `ADMIN_KEY`
`SUBMIT_KEY` `READER_KEY` `STORAGE` `REDIS_ADDR` `RATE_LIMIT_PER_MIN`
`DAILY_TOKEN_BUDGET` `AUDIT_PATH` `DEMO_ROOT` `CONFIG`。

## GitHub 真实仓库接入

`repos[]` 条目填上 `owner`/`repo` 后，该仓库的评审材料来自 GitHub：

- **diff**：`GET /repos/{o}/{r}/pulls/{n}`（.diff 媒体类型）
- **工作区**：head SHA 的 zipball 快照（解压到 `tmp/gh-workspaces/`，同 SHA 幂等复用）——
  裸 agent 的 `read_file` 在真实仓库代码上工作
- **回写**：评审结论自动发到 PR 评论页（`post_comments` 关闭则只存本地）
- GUI 里 GitHub 仓库带 `· GitHub` 徽标，PR 详情有直链；新建评审必须填 PR 号

## CI 自动触发（轮询模式）

中台本地运行（localhost），GitHub 无法主动连入——CI 用**轮询**实现同一事件语义：
`poll_sec` 间隔扫描 GitHub 开放 PR，**新 PR 自动提交评审、新提交（head SHA 变化）自动重跑**，
全部以 submitter 身份走同一条管线（治理/审批/审计照常生效）。日志前缀 `[ci]`。

- `branch_prefix: "drill/"` 只自动评审演练分支，不动你的真实 PR；留空 = 全部
- 中台部署到公网后可平替为 GitHub webhook 直推 `POST /webhook/pr`（管线完全复用）

试用流程：往演练分支 push 一个新提交 → 15 秒内中台日志出现 `[ci] ... 有新提交 -> 自动重新评审`
→ 评审完成后 PR 页出现新的结论评论。

## 真实 LLM 模式

方式一（推荐）：`config.json` 填 `llm` 段后重启终端 1：

```json
"llm": { "base_url": "https://你的网关/v1", "api_key": "sk-...", "model": "gpt-4o-mini" }
```

方式二：终端 1 设环境变量后启动：

```powershell
$env:OPENAI_BASE_URL = "https://你的网关/v1"
$env:OPENAI_API_KEY  = "sk-..."
$env:OPENAI_MODEL    = "gpt-4o-mini"        # 或网关支持的任意模型
python examples/pr-review-new/agent/agent.py
# 启动日志显示 mode=openai 即生效；GUI 顶栏芯片同步显示
```

agent 会真实流式调用 LLM，评审中可自主 `read_file` 查看仓库源码（工具循环，
最多 `max_tool_rounds` 轮，路径限制在 PR 工作区内）。

## GUI 角色验证（治理防线人工试用）

顶栏切换视角（admin / submitter / reader），所有提交与审批动作以当前角色
凭据经控制面裁决——越权会被控制面**真实 403 拒绝**，GUI toast 展示：

| 角色 | 能做 | 被拒（403） |
|---|---|---|
| admin | 建 PR · 提交 · 审批 · 查审计 · 查指标 | — |
| submitter | 建 PR · 提交 · 直播 · 看结果 | 审批 / 审计 / 指标 |
| reader | 看板 · 直播 · 看结果 | 提交 / 审批 / 审计 / 指标 |

三个 Agent 行为也可逐一验证：

| Agent | 白名单 | 审批 | 验证方式 |
|---|---|---|---|
| pr-reviewer | read_file | 免 | svc-orders 建 PR，直跑 |
| pr-reviewer-prod | read_file | 任务级 | svc-payments 建 PR → pending_approval → approve/reject |
| auto-fixer | +write_file | 任务级 | 新建 PR 时 Agent 下拉显式选择 |

## 故事五阶段 → 实现落点

| 阶段 | 实现 |
|---|---|
| 0 管理员初始化 | [bootstrap.go](bootstrap.go)：幂等注册 3 个评审 Agent（角色模板 + 风险声明在 spec） |
| 1 CI 触发 | [pipeline.go](pipeline.go) `submitPR`：Idempotency-Key=PR 号、session_id 按 PR 归因、治理守门（限流+预算） |
| 2 执行+直播 | 裸 agent [agent/agent.py](agent/agent.py)（SSE 合同 + 工具循环）；[api.go](api.go) `streamEvents` 直播代理（历史回放+增量） |
| 3 审批闸门 | 控制面原生 `pending_approval`；GUI approve/reject（按角色凭据转发） |
| 4 结果回写 | `writeback`：终态拉全量事件流聚合，结论（APPROVE/REQUEST_CHANGES）回写 PR；审计 JSONL 按 session 查账 |

## 可演练的可靠性场景

- **幂等**：`curl -X POST localhost:8090/webhook/pr -d '{"repo":"svc-orders","pr":101}'`（重复调用返回原任务）
- **重试自愈**：杀掉终端 1 的 agent → 任务 `AGENT_UNREACHABLE` 指数退避重试 → 重启 agent 后自动续跑成功
- **限流**：把 `RATE_LIMIT_PER_MIN` 调到 3 后连续建 PR → 第 4 个 429 `RATE_LIMITED`
- **审批驳回**：svc-payments PR 在 pending_approval 时 reject → cancelled（零资源消耗）

## CI 接入（预留）

- 契约：`POST /webhook/pr`，体 `{repo, pr, title, workspace?, diff?}`；`workspace`
  为中台可读的 PR 检出目录（自托管 runner 同机），`diff` 为 `git diff base..head`
  产物。响应 202 + `{task_id, task_status, session_id}`；重复调用幂等。
- 结果消费：轮询 `GET /api/board`（review.conclusion 字段）或订阅
  `GET /api/board/prs/{num}/events`（SSE）。

## 环境变量（覆盖 config.json 同名字段）

### 中台（终端 2）

| 变量 | 默认 | 说明 |
|---|---|---|
| PORT | 8090 | 中台监听端口 |
| AGENT_ADDR | 127.0.0.1:8091 | 裸 agent 地址 |
| ADMIN_KEY / SUBMIT_KEY / READER_KEY | admin-key / submit-key / reader-key | 三角色凭据（只在中台进程内） |
| STORAGE / REDIS_ADDR | memory | 控制面存储（redis = 重启不丢任务） |
| RATE_LIMIT_PER_MIN / DAILY_TOKEN_BUDGET | 60 / 5000000 | 治理护栏 |
| AUDIT_PATH | tmp/pr-new-audit.jsonl | 审计 JSONL |
| DEMO_ROOT | . | testdata 解析基点（默认仓库根运行） |

### 裸 agent（终端 1）

| 变量 | 默认 | 说明 |
|---|---|---|
| OPENAI_BASE_URL | https://api.openai.com/v1 | 任何 OpenAI 兼容网关 |
| OPENAI_API_KEY | —（空 = mock 模式） | 网关 key |
| OPENAI_MODEL | gpt-4o-mini | 模型名（按网关支持填写） |
| LLM_TIMEOUT_SEC | 180 | LLM 调用超时 |

## 边界与解耦

- 中台只 import agentflow 公开面（`agentflow`），不触碰库 `internal/**`
- 执行面只认 SSE 合同（[docs/protocol/sse.md](../../../docs/protocol/sse.md)），
  与控制面语言解耦——它不知道控制面存在，也能被任何符合合同的调用方驱动
- 工具白名单经 `AgentSpec.Config` 透传给 agent 强制执行；风险决策在管理端
  spec（乐观锁变更、审计留痕），调用方不能自选
- 与旧 `examples/pr-review` 的差异：无 claude CLI、无 docker、无自定义
  Runtime——只用内置 `python-http` 直连裸 agent，验证最小接入成本
