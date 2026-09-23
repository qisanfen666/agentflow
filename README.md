# agentflow

[English](README_EN.md) | 简体中文

**agentflow** 是一个面向 LLM/Agent 任务的控制面（control plane）Go 库：将**调度、排队、沙箱隔离、审计与治理**收敛为可 import 的基础设施，借鉴 Kubernetes 的分层思路——控制面管理生命周期与路由，不提供 AI 能力本身。

## 定位

**提供的能力**

- Agent 注册与版本化（乐观锁、软删除、版本锁定执行）
- 任务提交、状态机驱动与可靠队列（at-least-once 语义）
- Runtime 路由：进程直连（python-http）与一次性 Docker 沙箱容器
- SSE 输出透传（断线重连含历史回放）
- 工具注册表（JSON Schema 校验、MCP 格式导出）
- 可观测性：append-only 审计、Prometheus 指标、OTel 链路追踪
- 治理：API Key 认证授权、限流与 token 预算、审批流

**不提供的能力**：模型推理、Prompt 编排、Agent 业务逻辑。执行面是独立的用户进程，只需实现 [HTTP 端点 + SSE 协议](docs/protocol/sse.md)。

## 快速开始

### 库形态：嵌入既有 gin 应用

```go
panel, err := agentflow.New(agentflow.Config{
    Storage: agentflow.StorageConfig{
        Driver: "redis", // 或 "memory"
        Redis:  agentflow.RedisConfig{Addr: "localhost:6380"},
    },
    Runtimes: []string{"python-http", "docker"},
})
if err != nil { log.Fatal(err) }

panel.Mount(r.Group("/agentflow")) // 挂载到宿主子路径；Server.Addr 留空则不自行监听
panel.Start(ctx)                    // 启动 worker / delayed 搬运等后台组件
defer panel.Stop(ctx)               // 幂等，按序关停
```

完整可运行示例见 [examples/embed](examples/embed/main.go)。

### 独立部署形态

```powershell
go build -o agentflow-server.exe ./cmd/agentflow-server
$env:AGENTFLOW_MODE = "redis"; $env:AGENTFLOW_ADDR = ":8080"
./agentflow-server.exe
```

两种形态共享同一个 `Panel`，区别仅在 HTTP 监听归属（配置 `Server.Addr` 是否留空）。

### 执行面（Agent 侧）合同

任意语言实现一个 HTTP 端点，返回如下 SSE 流即可接入：

```
data: {"type":"token","content":" Hel"}
data: {"type":"usage","prompt_tokens":42,"completion_tokens":33,"model":"demo"}
data: {"type":"done","task_id":"t_9f3k2"}
data: [DONE]
```

零依赖参考实现：[examples/python-agent/agent.py](examples/python-agent/agent.py)。
协议细节与错误分类见 [docs/protocol/sse.md](docs/protocol/sse.md)、[docs/contracts/](docs/contracts/)。

## 架构

```
客户端 ──HTTP/SSE──► Panel（门面：New 装配 / Start-Stop 生命周期）
                      │
     ┌────────────────┼──────────────────────────────────────────┐
     │ api         REST 端点 + SSE 透传（可 Mount 进宿主应用）        │
     │ dispatch    状态机驱动执行、事件流 Hub（历史回放 + 唤醒）        │
     │ engine      worker + queue：claim/ack/nack、退避重投、         │
     │             delayed->ready 搬运、可见性超时回收（崩溃自愈）       │
     │ runtime     python-http（直连）| docker（一次性沙箱容器）        │
     │ registry    工具 JSON Schema 编译、参数校验、MCP 导出           │
     │ policy      提交前治理规则链（限流 / 预算 / 审批触发）           │
     │ storage     memory | redis，接口即合同（共享合同测试守护）       │
     └────────────────┼──────────────────────────────────────────┘
                      ▼ SSE（token 级增量）
              执行面 Agent（用户进程，任意语言）
```

任务主流程：提交（锁 agent 版本 + 幂等占位 + 治理守门）→ 入队 → worker 领取 →
dispatcher 驱动 pending→running→终态 → runtime 调执行面 → token 流经 Hub 实时透传 →
终态落库 + usage 归集。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | /health | 健康检查（探活豁免认证） |
| POST | /api/v1/agents | 创建（version=1） |
| GET | /api/v1/agents | 列出未删除 Agent |
| GET | /api/v1/agents/{id} | 当前版本详情 |
| PUT | /api/v1/agents/{id} | 更新（base_version 乐观锁，创建新版本） |
| DELETE | /api/v1/agents/{id} | 软删除（存量任务继续执行） |
| GET | /api/v1/agents/{id}/versions | 版本历史（升序） |
| POST | /api/v1/tasks | 提交（Idempotency-Key 幂等，锁定 agent_version） |
| GET | /api/v1/tasks/{id} | 状态查询 |
| DELETE | /api/v1/tasks/{id} | 取消（终态再删 409） |
| POST | /api/v1/tasks/{id}/approve | 审批通过（仅 admin；放行入队） |
| POST | /api/v1/tasks/{id}/reject | 审批驳回（仅 admin；→ cancelled） |
| GET | /api/v1/tasks/{id}/stream | SSE 订阅输出（断线重连有历史回放） |
| POST | /api/v1/tools | 注册工具（JSON Schema 参数） |
| GET | /api/v1/tools | 列出工具 |
| POST | /api/v1/tools/validate | 按工具 schema 校验参数 |
| GET | /api/v1/tools/mcp | 全量导出 MCP tools 清单 |
| GET | /api/v1/tools/{id}/mcp | 单个工具导出 MCP 格式 |

完整定义（含请求/响应 schema）：[docs/api/openapi.yaml](docs/api/openapi.yaml)。

## 可靠性语义

- **状态机**（[task-state.md](docs/contracts/task-state.md)）：pending → pending_approval →
  running → succeeded/failed/timeout/cancelled；终态不可逆由**存储层**原子保证
  （memory 互斥锁 / redis WATCH 乐观事务），并发取消与执行收尾竞态时先落者胜。
- **错误分类**（[error-taxonomy.md](docs/contracts/error-taxonomy.md)）：可重试
  （AGENT_UNREACHABLE/AGENT_TIMEOUT）按指数退避重投（base×attempt²，默认 3 次）；
  不可重试（协议违规等）直达终态。
- **at-least-once**：任务经 ready/processing/delayed 三列表 + 在途租约流转；
  消费者崩溃后可见性超时回收重投，running→pending 归一化自愈。重启续跑，任务不丢。
- **提交幂等**：Idempotency-Key 原子占位（SETNX），重复提交返回原任务。
- **版本锁定**：任务固化 (agent_id, version)，Agent 更新不影响在途任务；
  更新走 base_version 乐观锁，冲突 409。

## 可观测性

三种信号，各管一件事，按配置独立开关（未启用零开销）：

| 信号 | 开启方式 | 载体与用途 |
|---|---|---|
| **审计** | `Observability.AuditLogPath` | append-only JSONL：全链路动作埋点（提交/执行/重试/终态/审批 + Agent/Tool CRUD）。终态带 usage 汇总与 error_code；`session_id` 归因明细在此，供 ELK 等聚合 |
| **指标** | `Observability.MetricsEnabled` | `GET /metrics`（私有 registry，不污染宿主全局）：tasks_total{status}、retries_total、tokens_total{direction,model}、task_duration 直方图。高基数（session/task id）不进 label，只进审计明细 |
| **追踪** | `Observability.OTLPEndpoint` | OTLP gRPC 导出（Jaeger/Tempo 直接收）。API 根 span → task.execute → agent.http 三层；出站注入 W3C traceparent，执行面 Agent 可续链。跨队列因果：traceparent 随任务记录持久化，worker 侧以 span Link 挂回（Jaeger 中显示为 FollowsFrom 引用） |

观测栈一键起（Prometheus 抓取 + Grafana 四面板看板已预置）：

```powershell
docker compose -f deploy/docker-compose.yml up -d
# Grafana http://localhost:3300 (admin/admin) · Prometheus http://localhost:9090
```

告警立场：控制面只产生信号（指标/审计），通知渠道与告警规则归消费侧
（Alertmanager/Grafana Rules），不内建 webhook。

## 治理

三层防线，从外到内：

| 防线 | 机制 | 拒绝语义 |
|---|---|---|
| **认证授权** | X-API-Key + 三角色方法级权限矩阵（admin 全权 / submitter 提交消费 / reader 只读） | 401 未认证 / 403 越权 |
| **提交守门** | Policy 规则链（K8s admission 同构）：限流令牌桶 + token 日预算（按实际 usage 回报扣减，自然日重置） | 429 `RATE_LIMITED`（带 Retry-After）/ `BUDGET_EXCEEDED` |
| **审批闸门** | `AgentSpec.require_approval` 声明高危：任务提交即 `pending_approval`（不入队不执行），仅 admin 可 approve 放行 / reject 驳回 | 待审任务对执行面不可见；审批权不归调用方 |

风险决策权归属管理端（Agent spec 经乐观锁变更、审计留痕），不交给调用方自选。
嵌入形态下认证可整体替换：`Dependencies.Auth` 注入宿主中间件即可。

## 测试与冒烟

```powershell
go test ./...                 # 单元 + 合同 + Redis 集成（不可达自动 skip）
./scripts/smoke-m1.ps1        # 全端点行为（需 python3 + server 实例）
./scripts/smoke-m2.ps1        # 强杀进程 → Redis 数据仍在 → 重启续跑（需 docker redis）
./scripts/smoke-m3.ps1        # 双 Runtime 路由 + 容器零残留 + MCP（需 docker）
./scripts/smoke-m4.ps1        # 库形态嵌入：宿主路由与挂载控制面共存（需 python3）
./scripts/smoke-m5.ps1        # 观测栈闭环：指标进 Prometheus + 看板进 Grafana（需 docker）
./scripts/smoke-m6.ps1        # 治理三层防线：认证/审批/预算/限流（需 python3）
```

## 配置参考（Config）

| 字段 | 默认 | 说明 |
|---|---|---|
| Server.Addr | 空（不监听） | 监听地址；留空 = 嵌入模式，路由交宿主 |
| Server.Mode | 空 | gin 模式（debug/release/test） |
| Storage.Driver | memory | memory \| redis |
| Storage.Redis | localhost:6379 | redis 连接（Driver=redis 时生效） |
| Queue.MaxRetries | 3 | 可重试错误的最大重试次数 |
| Queue.RetryBackoffBaseMs | 1000 | 退避基数（延迟 = base × attempt²） |
| Queue.VisibilityTimeoutSec | 300 | 在途租约/可见性超时 |
| Runtimes | [python-http] | 声明启用的 Runtime；未知/重复报错 |
| Sandbox | — | docker Runtime 的资源限制（只读根文件系统/内存/CPU） |
| Observability.AuditLogPath | 空（关） | 审计 JSONL 路径 |
| Observability.MetricsEnabled | false | 挂 GET /metrics |
| Observability.OTLPEndpoint | 空（关） | OTLP gRPC 地址（如 localhost:4317） |
| Observability.ServiceName | agentflow | OTel service.name |
| Auth.Enabled | false | 启用 API Key 认证授权 |
| Auth.Keys | — | `[{key, roles}]`，角色：admin/submitter/reader |
| Governance.RateLimitPerMin | 0（不限） | 全局提交速率上限/分钟 |
| Governance.DailyTokenBudget | 0（不限） | 自然日 token 预算（按实际 usage 扣减） |

## 目录结构

```
agent.go              Panel 门面（公开 API）
config.go             Config 定义与默认值
model/                领域模型：AgentSpec/Task 状态机/错误码/ToolDef
storage/              存储接口 + memory/redis 实现 + 合同测试
runtime/              python-http / docker 执行环境（SSE 内核共用）
internal/api/         REST 端点 + SSE 透传 + 认证授权
internal/dispatch/    执行编排 + 事件流 Hub
internal/engine/      worker + 内存/Redis 可靠队列
internal/registry/    工具 schema 编译 + MCP 导出
internal/policy/      治理规则链（限流 / 预算）
internal/observability/ 审计 / 指标 / 追踪
cmd/agentflow-server/ 独立部署二进制
examples/             embed（嵌入示例）、python-agent（零依赖执行面）
docs/                 合同文档、SSE 协议、OpenAPI
deploy/               观测栈（Prometheus + Grafana）
scripts/              各验收场景冒烟脚本
```
