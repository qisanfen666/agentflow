# agentflow

Agent 领域的"控制面"库：把 LLM/Agent 任务的**调度、排队、沙箱、审计**收敛为可 import 的 Go 库。
借鉴 Kubernetes 的分层思路——控制面管生命周期与路由，不提供 AI 能力本身。

**它管什么**：Agent 注册与版本化、任务提交与状态机、可靠队列（at-least-once）、Runtime 路由
（直连 / Docker 沙箱）、SSE 输出透传、工具注册表（JSON Schema + MCP 导出）。

**它不管什么**：模型推理、Prompt 编排、Agent 业务逻辑。执行面是你自己的进程，只需实现
[HTTP 端点 + SSE 协议](docs/protocol/sse.md)。

## 快速上手

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

panel.Mount(r.Group("/agentflow")) // 挂到宿主子路径；Server.Addr 留空则不自行监听
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
     │ storage     memory | redis，接口即合同（共享合同测试守护）       │
     └────────────────┼──────────────────────────────────────────┘
                      ▼ SSE（token 级增量）
              执行面 Agent（用户进程，任意语言）
```

任务主流程：提交（锁 agent 版本 + 幂等占位）→ 入队 → worker 领取 → dispatcher 驱动
pending→running→终态 → runtime 调执行面 → token 流经 Hub 实时透传 → 终态落库 + usage 归集。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | /health | 健康检查 |
| POST | /api/v1/agents | 创建（version=1） |
| GET | /api/v1/agents | 列出未删除 Agent |
| GET | /api/v1/agents/{id} | 当前版本详情 |
| PUT | /api/v1/agents/{id} | 更新（base_version 乐观锁，创建新版本） |
| DELETE | /api/v1/agents/{id} | 软删除（存量任务继续执行） |
| GET | /api/v1/agents/{id}/versions | 版本历史（升序） |
| POST | /api/v1/tasks | 提交（Idempotency-Key 幂等，锁定 agent_version） |
| GET | /api/v1/tasks/{id} | 状态查询 |
| DELETE | /api/v1/tasks/{id} | 取消（终态再删 409） |
| GET | /api/v1/tasks/{id}/stream | SSE 订阅输出（断线重连有历史回放） |
| POST | /api/v1/tools | 注册工具（JSON Schema 参数） |
| GET | /api/v1/tools | 列出工具 |
| POST | /api/v1/tools/validate | 按工具 schema 校验参数 |
| GET | /api/v1/tools/mcp | 全量导出 MCP tools 清单 |
| GET | /api/v1/tools/{id}/mcp | 单个工具导出 MCP 格式 |

完整定义（含请求/响应 schema）：[docs/api/openapi.yaml](docs/api/openapi.yaml)。

## 可靠性语义

- **状态机**（[task-state.md](docs/contracts/task-state.md)）：pending → running →
  succeeded/failed/timeout/cancelled；终态不可逆由**存储层**原子保证
  （memory 互斥锁 / redis WATCH 乐观事务），并发取消与执行收尾竞态时先落者胜。
- **错误分类**（[error-taxonomy.md](docs/contracts/error-taxonomy.md)）：可重试
  （AGENT_UNREACHABLE/AGENT_TIMEOUT）按指数退避重投（base×attempt²，默认 3 次）；
  不可重试（协议违规等）直达终态。
- **at-least-once**：任务经 ready/processing/delayed 三列表 + 在途租约流转；
  消费者崩溃后可见性超时回收重投，running→pending 归一化自愈。重启续跑，任务不丢。
- **提交幂等**：Idempotency-Key 原子占位（SETNX），重复提交返回原任务。
- **版本锁定**：任务固化 (agent_id, version)，Agent 更新不影响在途任务；
  更新走 base_version 乐观锁，冲突 409。

## 测试与冒烟

```powershell
go test ./...                 # 单元 + 合同 + Redis 集成（不可达自动 skip）
./scripts/smoke-m1.ps1        # 全端点行为（需 python3 + server 实例）
./scripts/smoke-m2.ps1        # 强杀进程 → Redis 数据仍在 → 重启续跑（需 docker redis）
./scripts/smoke-m3.ps1        # 双 Runtime 路由 + 容器零残留 + MCP（需 docker）
./scripts/smoke-m4.ps1        # 库形态嵌入：宿主路由与挂载控制面共存（需 python3）
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

## 目录结构

```
agent.go              Panel 门面（公开 API）
config.go             Config 定义与默认值
model/                领域模型：AgentSpec/Task 状态机/错误码/ToolDef
storage/              存储接口 + memory/redis 实现 + 合同测试
runtime/              python-http / docker 执行环境（SSE 内核共用）
internal/api/         REST 端点 + SSE 透传
internal/dispatch/    执行编排 + 事件流 Hub
internal/engine/      worker + 内存/Redis 可靠队列
internal/registry/    工具 schema 编译 + MCP 导出
cmd/agentflow-server/ 独立部署二进制
examples/             embed（嵌入示例）、python-agent（零依赖执行面）
docs/                 合同文档、SSE 协议、OpenAPI
scripts/              各里程碑冒烟脚本
```
