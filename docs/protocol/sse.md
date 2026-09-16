# SSE 事件协议 v1

> Agent 执行面与控制面之间的唯一线上合同。评审通过后冻结为 v1。
> 配套合同：错误码表见 [error-taxonomy.md](../contracts/error-taxonomy.md)，任务状态见 [task-state.md](../contracts/task-state.md)。

## 1. 传输层

Agent 侧只需暴露一个端点：

- **`POST /run/stream`**
- 请求：`Content-Type: application/json`
- 响应：`Content-Type: text/event-stream`（SSE）
- 每个 SSE `data:` 行是一个 JSON 对象（下称**事件**）
- 流末尾以字面量结束：`data: [DONE]`
- 心跳：Agent 可发送注释行 `: ping`（建议间隔 15s），控制面必须忽略

请求体：

```json
{
  "task_id": "t_9f3k2",          // 控制面任务 ID（必填）
  "agent_config": { ... },        // AgentSpec 中 runtime 相关配置，原样透传（可选）
  "payload": { ... }              // 任务输入（必填，内部结构由 Agent 自定义）
}
```

## 2. 事件类型总表

| type | 语义 | 出现次数 | 说明 |
|---|---|---|---|
| `token` | 流式增量输出 | 0..N | 相对顺序 = 输出顺序 |
| `event` | 结构化里程碑 | 0..N | `thought` / `tool_call` / `tool_result`，供追踪回放 |
| `usage` | token 用量上报 | 0..N | 多次出现则由控制面累加 |
| `error` | 终止性错误 | 0..1 | 出现后流必须结束 |
| `done` | 成功结束标记 | 0..1 | 必须是最后一个 JSON 事件，之后紧跟 `[DONE]` |

**顺序保证**：`error` 与 `done` 互斥、最多一个、必须是最后一个 JSON 事件。其余事件类型可交错。

## 3. 事件定义

### 3.1 token

```json
{"type": "token", "content": " Hel"}
```

- `content`: string，UTF-8 增量片段，不得为空字符串

### 3.2 event

```json
{"type": "event", "event": "tool_call", "data": {"tool": "search", "args": {"q": "agent scheduler"}}}
```

- `event`: `thought` | `tool_call` | `tool_result`
- `data`: object。v1 只约定字段存在，**控制面不解释内部结构**，只透传与存储（回放用）

### 3.3 usage

```json
{"type": "usage", "prompt_tokens": 120, "completion_tokens": 35, "model": "gpt-4o"}
```

- 多次出现时控制面做**累加求和**
- `model` 可选，用于成本归因

### 3.4 error

```json
{"type": "error", "code": "TOOL_TIMEOUT", "message": "search timed out after 30s"}
```

- `code` 见错误分类学总表
- 可重试性由控制面按 code 判定，Agent 无需上报

### 3.5 done

```json
{"type": "done", "task_id": "t_9f3k2"}
```

## 4. 完整会话示例

```
data: {"type":"event","event":"thought","data":{"text":"需要先搜索"}}

data: {"type":"token","content":"根据"}
data: {"type":"token","content":"搜索结果"}
data: {"type":"event","event":"tool_call","data":{"tool":"search","args":{"q":"..."}}}
data: {"type":"event","event":"tool_result","data":{"ok":true}}
data: {"type":"token","content":"，结论是..."}
data: {"type":"usage","prompt_tokens":120,"completion_tokens":35}
data: {"type":"done","task_id":"t_9f3k2"}
data: [DONE]
```

失败示例：

```
data: {"type":"token","content":"正在调用"}
data: {"type":"error","code":"TOOL_TIMEOUT","message":"search timed out"}
data: [DONE]
```

## 5. 兼容性策略

- v1 字段**只增不改不删**
- 消费方必须忽略未知 `type` 与未知字段（前向兼容）
- 协议版本暂不显式协商（无版本头 = v1），破坏性变更出现时引入 `X-Protocol-Version`

## 6. 设计理由（面试要点）

**为什么 SSE 而不是 WebSocket？**
1. 通信本质是单向流（下发仅一个请求体，之后只有上行 token），WebSocket 的双向能力是浪费
2. SSE 是纯 HTTP，复用现有基础设施：LB、反向代理、认证中间件、超时策略全部直接可用
3. 断线语义简单：HTTP 语义天然表达"连接断=流断"
4. Agent 侧实现成本最低：一个普通 handler + 逐行 `yield`，任何语言/框架 30 行内完成

**为什么自定义 JSON 事件而不是复用 OpenAI 流式 chunk 格式？**
1. 控制面需要 `event`（追踪回放）和 `usage`（成本归因）这类控制类事件，OpenAI 格式没有这些槽位
2. 控制面是框架无关的，内部合同不能绑死在任何厂商格式上

**为什么用 `[DONE]` 字面量结束？**
- 与 OpenAI 流式惯例一致，Agent 作者零学习成本；且 JSON 域和结束标记域分离，解析器不用特判
