# 错误分类学 v1

> 错误类别决定两件事：(1) 是否重新入队重试 (2) 审计与告警级别。
> 本文是错误码的唯一权威来源，Go 侧以 `model` 包常量落地（单一常量源）。

## 1. 三大类

| 类别 | 判定标准 | 控制面行为 |
|---|---|---|
| **retryable**（可重试） | 瞬态故障：网络不可达、Agent 5xx、分发超时 | Nack → 重新入队（受 `max_retries` 约束） |
| **permanent**（不可重试） | 确定性失败：参数校验、工具业务错误、协议违约 | 直接置 `failed` |
| **violation**（安全违规） | 沙箱逃逸尝试、越权资源访问 | 置 `failed` + 审计告警（M5） |

## 2. 错误码总表

命名规则：大写下划线；前缀标识触发域（`AGENT_` / `TOOL_` / `SANDBOX_` / 控制面无前缀或 `CTRL_`）。

| code | 类别 | 触发方 | 典型场景 |
|---|---|---|---|
| `AGENT_UNREACHABLE` | retryable | 控制面 | 连接 Agent 失败（DNS 拒绝/连接拒绝） |
| `AGENT_TIMEOUT` | retryable | 控制面 | HTTP 分发超时 |
| `AGENT_INTERNAL` | retryable | 控制面 | Agent 返回 5xx |
| `AGENT_PROTOCOL_VIOLATION` | permanent | 控制面 | 流不符合 SSE 协议（非法 JSON / 缺 `[DONE]` / `error` 后仍有事件） |
| `INVALID_PAYLOAD` | permanent | 控制面 | 提交参数未通过 AgentSpec / ToolDef 校验 |
| `TOOL_TIMEOUT` | permanent | Agent | 工具执行超时 |
| `TOOL_EXEC_FAILED` | permanent | Agent | 工具业务错误（参数错、上游 4xx 等） |
| `SANDBOX_VIOLATION` | violation | 控制面/沙箱 | 只读违规、禁网违规、资源越限 |
| `RATE_LIMITED` | retryable | 控制面 | 触发限速，延迟退避后重试 |
| `CANCELLED` | permanent | 控制面 | 客户端取消 |
| `INTERNAL` | retryable | 控制面 | 控制面自身错误（兜底） |

**为什么 `TOOL_TIMEOUT` 不可重试？** 工具调用可能有副作用（写库、发请求）；Agent 侧单个工具失败时重跑整个任务会把副作用翻倍。v1 采取保守策略：任务级 at-least-once，工具级不重试。

## 3. Go 侧落地约定

1. 错误码以 `string` 常量集中定义在 `model` 包，**禁止在业务代码里散落字符串字面量**
2. 控制面内部错误传播：`fmt.Errorf("...: %w", err)` 包装，`errors.Is/As` 判定
3. 跨网络传输只传 `code + message`，不传 Go 错误对象
4. 每个错误码的可重试性编译期即可查（`model.IsRetryable(code)`），不靠运行时字符串比较散布逻辑
