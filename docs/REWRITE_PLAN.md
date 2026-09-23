# AgentFlow Control Plane —— 重写规划

> 状态：待约定（M0 之前先对齐开发约定）
---

## 一、需求理解

### 定位

**Agent 领域的 Kubernetes。** 一个可 import 的 Go 控制面库，本身不提供 AI 能力，只为任何 Agent 框架提供生产级基础设施：调度、排队、沙箱、审计、成本归因。

- 库形态：`import "github.com/you/agentflow"`，一键启动或嵌入已有项目
- 对开发者：不用关心 K8s / 队列 / 隔离 / 监控，一个 import 获得生产级 Agent 运行时
- 对企业：把不可控的 AI 调用转变为符合 IT 治理标准的白盒服务（RBAC / 审计日志 / 成本中心）

### 核心架构：控制面与执行面解耦

```
控制面 (Go 库)                       Agent 侧 (任何语言/框架)
Scheduler → Queue → Worker        统一合同：
  → Dispatcher (按 Runtime 路由)     POST /run/stream ← JSON
       ├─ python-http (HTTP 转发)      → SSE token 流 → [DONE]
       ├─ docker      (容器沙箱)
       └─ 自定义 Runtime (实现接口)
```

- Agent 接入门槛：写一个 ~30 行的 HTTP 适配器
- Runtime 扩展：实现 `Runtime` 接口（Type + Execute + Health）

### 三层

1. **控制面（Go Library）**：facade 入口、Gin API、调度（排队/限速/版本锁定/重试）、Runtime 路由、策略引擎（后期）
2. **执行面（Runtime）**：统一 SSE 合同，内置 python-http 与 docker 沙箱，支持自定义扩展
3. **数据面（后期）**：全链路追踪、Token 计数、审计日志、Prometheus

### 四期里程碑（需求来源，重排后见下文）

- Phase 1：Gin 路由、Agent CRUD、版本管理、Queue 抽象、配置管理
- Phase 2：Docker 沙箱、Tool Registry、MCP 兼容、多 Runtime
- Phase 3：OpenTelemetry、Tracing、审计日志、Token 计数、Prometheus、Session
- Phase 4：Policy Engine、RBAC、Rate Limiting、Cost Guardrails、审批流、灰度发布、K8s Operator

---

## 二、旧版病根诊断（重写要根治的问题）

1. **库和应用边界模糊**：所有包平铺在根目录且全公开，`main.go` 手工 DI —— 形态是"一个应用"，不是"可 import 的库"。
2. **接口先于消费者定义**：集中式 `interface.go` 违背 Go 惯例。接口应由消费者定义（engine 需要什么能力，接口就写在 engine 里）。
3. **横向分期，没有垂直切片**：Phase 1 把所有层骨架一次搭完，没有端到端可运行的东西，后期各层对不齐。
4. **合同没有先行**：SSE 协议只定义了 `token` + `[DONE]`，而后期的 tracing / token 计数 / 审计全部依赖事件类型，不定稿必然大返工。

---

## 三、重写的核心架构决策

### 决策 1：目录结构 —— 库形态收口

```
agentflow/                        # module: github.com/you/agentflow
├── agent.go                      # 根包 facade：New / Start / Stop / Mount
├── config.go                     # 根包 Config（用户唯一需要碰的配置）
├── model/                        # 公开：Task / AgentSpec / ToolDef 等领域类型
│                                 #   （出现在公开接口签名里的类型必须公开）
├── runtime/                      # 公开：扩展点① —— 用户实现自定义 Runtime
│   ├── runtime.go                #   Runtime 接口定义在这里
│   ├── pythonhttp.go
│   └── docker.go
├── storage/                      # 公开：扩展点② —— 可替换存储（memory / redis）
├── internal/
│   ├── engine/                   # scheduler / worker / dispatcher + 它们消费的接口
│   ├── registry/                 # tool registry + MCP 转换
│   ├── observability/            # M5: audit / tracing / metrics
│   ├── policy/                   # M6: RBAC / rate limit / guardrails
│   └── api/                      # gin handlers + middleware
├── cmd/agentflow-server/         # 开箱即用的演示二进制（不再是主体）
└── examples/
    ├── python-agent/             # 参照 Agent（实现合同的 ~30 行示例）
    └── embed/                    # 嵌入已有 gin 项目的示例
```

**公开/内部判定规则**：用户可能实现或复用的 → public（`runtime`、`storage`、`model`）；纯实现细节 → `internal`（`engine`、`api`、`registry`）。

### 决策 2：依赖方向严格单向

```
model(零依赖) ← storage / runtime ← engine ← api ← agent(facade)
```

禁止反向 import，禁止跨层横向 import（engine 不许碰 api）。

### 决策 3：垂直切片推进

每个里程碑结束都是端到端可运行、可演示的状态，而不是"又多了几个文件的骨架"。

### 决策 4：M1 用内存实现，零外部依赖起步

队列/存储先给内存版（同时也是测试的 fake），Redis 在 M2 才换入。本地开发和单测不需要起 Redis。

---

## 四、M0 必须先定稿的四个合同

1. **SSE 事件协议 v1**（最重要，M5 全依赖它）：

```
data: {"type":"token","content":"..."}
data: {"type":"event","event":"thought|tool_call|tool_result","data":{...}}
data: {"type":"usage","prompt_tokens":120,"completion_tokens":35}
data: {"type":"error","code":"TOOL_TIMEOUT","message":"..."}
data: {"type":"done","task_id":"..."}
data: [DONE]
```

预留 `event` 和 `usage` 两类事件，后期回放/计费不用动协议。

2. **Task 状态机**：`pending → running → succeeded | failed | cancelled | timeout`，含 retry 语义（状态迁移责任方、重试上限、退避策略）。
3. **错误分类学**：可重试（网络/5xx）vs 不可重试（参数校验）vs 沙箱违规 —— 决定 Ack/Nack 行为。
4. **AgentSpec 版本语义**：版本不可变，更新 = 创建新版本号（原子递增），语义先写进文档。

外加：OpenAPI yaml、config 结构定稿、Makefile、CI。

---

## 五、里程碑（垂直切片重排）

| 里程碑 | 内容 | 完成标志 |
|---|---|---|
| **M0 合同与骨架** ✅ 2026-09-08 | 目录、SSE 协议文档、四合同、OpenAPI（14 端点）、Config（CI/Makefile 推迟至建 git 仓库） | 文档评审通过 |
| **M1 端到端最小切片** ✅ 2026-09-14 | 内存存储 + 直接分发（无队列）+ Agent CRUD + 提交 + SSE 透传 + 幂等/取消 | examples/python-agent 跑通全链路 demo |
| **M2 持久化与队列** ✅ 2026-09-15 | storage 双实现（Lua 原子版本/幂等 SETNX）、engine Queue 接口 + Worker 循环（Ack/Nack、指数退避重试、可见性回收）、RedisQueue（BLMOVE/降级 BRPopLPush、ZSET 延迟退避、在途租约）、提交即入队 | 集成测试过（共享 6380 容器，按包分 DB）+ 冒烟：杀 server 重启任务不丢 |
| **M3 执行面** ✅ 2026-09-16 | runtime 公开化：python-http + Docker 沙箱（CLI 方案：一次性容器生命周期，只读/内存/CPU 限制，网络禁用明确不支持）、SSE 内核抽取复用、Tool Registry（jsonschema v6 校验 + ToolStore 双实现 + MCP 导出 2 端点） | 冒烟：双 Runtime 同 server 各跑一任务成功 + 容器零残留 + 工具全流程 409/400/MCP |
| **M4 库收口 v0.1** ✅ 2026-09-19 | facade `New/Start/Stop/Mount`、优雅关停、README、examples 补全（embed 嵌入形态）、终态不可逆下沉存储层（修复取消复活竞态） | 打 tag v0.1，公开 API 冻结 |
| **M5 可观测性** ✅ 2026-09-20 | 审计落地（append-only JSONL，11 类动作全链路埋点，session_id 归因）、Prometheus 指标（私有 registry：tasks/retries/tokens/duration，token 计数按 model 分维）、OTel 追踪（三层 span + traceparent 穿透执行面；队列两侧 Link 补接待做）、观测栈 compose（Prometheus+Grafana 四面板预置） | 冒烟：指标进 Prometheus（126=3×42 对账）+ 看板预置进 Grafana |
| **M6 治理** ✅ 2026-09-23 | 认证授权（API Key + 三角色方法级矩阵，Auth 中间件可插拔）、Policy 规则链（限流令牌桶 + token 日预算按实扣减，429 守门）、审批流（pending_approval 状态机 + admin 审批端点 + 队列闸门语义） | 冒烟：三层防线 17 项断言全过（认证三态/approve·reject/预算·限流 429） |

顺序要点：**facade 收口提到 M4 而不是拖到最后** —— "库形态"是项目身份，必须尽早用真实 facade 使用体验验证目录结构。

**每个里程碑 DoD**：单测 + 集成测 + 路线图更新 + demo 可跑。

---

## 六、关键接口草图

engine 按需定义自己消费的接口（不再有集中式 interface.go）：

```go
// internal/engine/ports.go —— engine 要什么才定义什么
type Queue interface {
    Enqueue(ctx context.Context, task model.Task) error
    Dequeue(ctx context.Context) (model.Task, error) // 阻塞
    Ack(ctx context.Context, id string) error
    Nack(ctx context.Context, id string, retryable bool) error
}
```

用户侧体验（M4 验收标准）：

```go
panel := agent.New(agent.Config{Redis: "localhost:6379"})
panel.Start(ctx)
defer panel.Stop(ctx)

// 或嵌入已有服务
r.Mount("/agentflow", panel.Router())
```

---

## 七、风险与对策

- **Redis 强依赖拖慢开发** → M1 内存实现打底，接口隔离，M2 才换
- **Docker 沙箱测试慢** → Runtime 用 fake 做单测，真实 Docker 测试打 build tag 分离
- **API 范围蔓延** → M4 打 v0.1 冻结公开 API，后续变更走版本化
- **旧文档误导** → 旧 ROADMAP.md / plan_ultimate.md 归档留作需求来源，本文档为新路线图

---

## 开发约定（2026-09-08 对齐）

> 项目定位补充：本项目属于**技术尝试**级别，后续可能用于面试相应岗位，所有技术选型必须可解释。

### 1. 写前确认（程序化推进）

- 每轮写代码前，先列出本轮交付物清单：文件清单、每个文件职责、关键接口/函数签名、模块依赖关系
- 用户确认后才动手写码；未确认不写
- 每轮结束对照"清单 vs 实际产出"，防止范围漂移

### 2. 模块清晰

- 单一职责：一个文件/包只做一件事，能用一句话解释
- 依赖严格单向（见决策 2），禁止横向 import
- 命名、错误处理风格全项目统一；枚举/状态常量只从单一常量源引用，不散落硬编码

### 3. 技术可解释（面试导向）

- 每引入一个技术（Redis、Docker SDK、OTel 等）必须能回答三问：**为什么用它？替代方案是什么？不用会怎样？**
- 关键设计决策（SSE 合同、BRPopLPush 可靠队列、版本锁定、沙箱策略等）随代码沉淀为简短 ADR（技术决策记录），面试可直接引用
- 宁可少而精，不堆砌技术栈；每个里程碑的技术点都能白板讲清

### 工作节奏

```
约定对齐（本文档） → M0 合同定稿 → 之后每一轮：
    写前列清单 → 用户确认 → 写码 + 测试 → 更新路线图 → 下一轮
```
