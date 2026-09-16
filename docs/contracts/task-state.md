# Task 状态机与重试语义 v1

> 定义 Task 生命周期、状态迁移责任方、重试/幂等/取消语义。
> 状态常量唯一来源：`model` 包（代码落地在 M1）。

## 1. 状态集

```
pending → running → succeeded
                ↘ failed
                ↘ timeout
                ↘ (可重试) pending   # 重新入队
pending/running → cancelled         # 客户端显式取消
```

- 非终态：`pending`、`running`
- **终态**（不可再迁移）：`succeeded`、`failed`、`timeout`、`cancelled`

## 2. 迁移表

| from | 触发条件 | to | 责任方 |
|---|---|---|---|
| — | `POST /api/v1/tasks` 提交成功 | `pending` | API handler |
| `pending` | worker 取出任务、锁定版本、开始分发 | `running` | worker |
| `running` | 收到 `done` 事件 | `succeeded` | worker |
| `running` | 不可重试错误（permanent/violation） | `failed` | worker |
| `running` | 可重试错误 且 `attempt_count < max_retries` | `pending` | worker（重新入队） |
| `running` | 可重试错误 且已达上限 | `failed` | worker |
| `running` | 执行超过任务超时 | `timeout` | worker（context deadline） |
| `pending` | `DELETE /api/v1/tasks/:id` | `cancelled` | API handler（从队列移除） |
| `running` | `DELETE /api/v1/tasks/:id` | `cancelled` | API handler 标记，worker 尽力传播 |

**约束**：任何对终态的迁移请求都是 bug，实现层必须拒绝并记录（而非静默覆盖）。

## 3. 重试语义

- `attempt_count` 从 1 开始；每次可重试失败 +1 后重新入队
- 退避策略：`backoff = base_ms * attempt²`（M1 内存实现先固定间隔，M2 引入指数退避）
- 上限：`queue.max_retries`（默认 3）；超限即 `failed`，`error.code = INTERNAL` 或具体 retryable 码
- 语义级别：**at-least-once**。Agent 侧不要求幂等，重试 = 完整重跑任务

## 4. 幂等（提交侧）

- 客户端可带 `Idempotency-Key` header 提交任务
- 相同 key 在窗口期内重复提交 → 返回原 task（不新建）
- M1 内存实现用 map + TTL；M2 用 Redis SETNX

## 5. 取消语义

- v1 仅显式取消：`DELETE /api/v1/tasks/:id`
- `pending`：直接从队列摘除，置 `cancelled`
- `running`：置 `cancelled`；worker 通过 context 感知后丢弃后续事件（尽力传播，不保证 Agent 立即停止）
- Agent 侧取消传播（主动通知 Agent 中断）**推迟到 v2**，v1 不做

## 6. 设计理由（面试要点）

**为什么单独设 `timeout` 而不是并入 `failed`？**
运维语义不同：`timeout` 大概率是环境问题（Agent 卡死、网络黑洞），需要独立监控指标与告警阈值；`failed` 是业务语义失败。合并会让"环境故障率"这个关键指标失真。

**为什么重试上限在队列配置而非任务级？**
控制面职责是保护系统不被毒丸任务拖垮（防止无限重试风暴），这是平台级策略；任务级自定义优先级属于后续演进。
