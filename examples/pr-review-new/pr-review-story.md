# agentflow × 裸 Agent：企业 PR Review 控制面场景

> 本文件为 demo 演示故事线（pr-review-new 实现，试用指引见 [README.md](README.md)）

## 验证命题

**一个"裸 agent"（最基础的 LLM 调用 + read_file/write_file 工具循环，零依赖
Python 进程）只需实现一个 SSE 端点（`POST /run/stream`），接入 agentflow 控制
面后即获得全部生产化能力：排队、审批、直播、回写、预算、审计。**
控制面与执行面完全解耦——agent 不知道控制面存在，控制面不知道 agent 用什么
模型、什么语言。

## 阶段 0：管理员初始化（一次性）

中台后端（Go/gin）嵌入 agentflow，只用内置 `python-http` runtime：

```go
panel, _ := agentflow.New(agentflow.Config{
    Server:  agentflow.ServerConfig{Mode: gin.ReleaseMode},     // Addr 留空：嵌入
    Storage: agentflow.StorageConfig{Driver: "redis", Redis: agentflow.RedisConfig{Addr: "redis:6379"}},
    Runtimes: []string{"python-http"},                          // 标准合同直连裸 agent
    Governance: agentflow.GovernanceConfig{RateLimitPerMin: 60, DailyTokenBudget: 5_000_000},
})
panel.Mount(r.Group("/agentflow"))
```

注册 Agent（admin key，角色模板；风险全部声明在 spec，调用方不能自选）：

| Agent | 工具白名单 | require_approval | 用途 |
|---|---|---|---|
| pr-reviewer | read_file（纯只读） | false | 普通仓库 |
| pr-reviewer-prod | read_file | true | 生产仓库（任务级审批） |
| auto-fixer | +write_file | true | 自动修复（高危） |

## 阶段 1：开发者提 PR → 自动触发

CI webhook → 中台 → 以 submitter 身份 POST /agentflow/api/v1/tasks：

```json
{
  "agent_id": "pr-reviewer",
  "session_id": "pr-svc-orders-101",              // 按 PR 归因（成本按 PR 查账）
  "payload": {"repo": "svc-orders", "pr": 101, "base": "main",
               "workspace": "…/testdata/svc-orders", "diff": "…"},
  "timeout_sec": 600
}
```
Idempotency-Key: pr-svc-orders-101（webhook 重发不重复建任务）

治理链：认证（三角色矩阵）→ 限流 → 预算 → 入队 → 202。

## 阶段 2：执行 + 直播

worker → python-http runtime → 裸 agent（独立 Python 进程）→
OpenAI 兼容流式调用（无 key 时 mock 假 LLM 降级）+ 工具循环
（read_file 读 PR 工作区，路径越界防护）→
thought/tool_call/tool_result/token/usage 事件流 → SSE 推给 PR 页
（开发者实时看 agent 读哪个文件、想什么）→
usage 实扣预算 + 审计逐条落 JSONL。

## 阶段 3：高危仓库走审批

pr-reviewer-prod / auto-fixer → 任务 pending_approval（零资源）→
中台审批中心（admin）→ approve → 入队；reject → cancelled。
submitter/reader 视角审批被控制面 403 真实拒绝（GUI 可验证）。

## 阶段 4：结果回写

succeeded + 事件流聚合（问题列表/风险分级 + APPROVE/REQUEST_CHANGES）→
中台回写 PR 评论。指标：timeout 率、token 消耗、失败率（/metrics）；
审计按 session 查每个 PR 的完整账（JSONL 尾部视图，admin 专属）。

## 人工试用流程（对照实现）

```
终端1: python examples/pr-review-new/agent/agent.py        # 裸 agent（mock 或配 OPENAI_*）
终端2: go run ./examples/pr-review-new                     # 中台 + GUI :8090
浏览器: http://localhost:8090
  1. svc-orders 建 PR → 直播看 thought/tool_call/token → 回写结论
  2. svc-payments 建 PR → pending_approval → approve/reject
  3. 顶栏切角色：reader 提交被 403 / submitter 审批被 403 / admin 全过
  4. 杀掉终端1 → 任务退避重试 → 重启 agent 自愈
```
