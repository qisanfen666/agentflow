// Package policy 实现提交前治理（M6）：限流与成本护栏收敛为统一的规则链。
// 架构同构 K8s admission：新增护栏 = 实现 Rule 加入 Chain，而不是在 handler 里散落 if。
// 拒绝语义都发生在提交时刻（入队前）——队列内任务不受影响。
package policy

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/qisanfen666/agentflow/model"
)

// 治理拒绝码（HTTP 层统一映射 429）。
const (
	CodeRateLimited    = "RATE_LIMITED"
	CodeBudgetExceeded = "BUDGET_EXCEEDED"
)

// Verdict 单条规则的裁决。
type Verdict struct {
	Allowed    bool
	Code       string // 拒绝码（Allowed=false 时必有）
	Message    string
	RetryAfter time.Duration // RATE_LIMITED 时建议的重试等待
}

// allow 放行裁决。
var allow = Verdict{Allowed: true}

// Rule 治理规则：对一次提交给出裁决。
type Rule interface {
	Name() string
	Evaluate(ctx context.Context, task model.Task) Verdict
}

// Chain 规则链：全部放行才 Allow，首个拒绝短路返回（按序执行，限流在前预算在后：
// 便宜的检查先跑）。
type Chain []Rule

// Evaluate 返回链的最终裁决。
func (c Chain) Evaluate(ctx context.Context, task model.Task) Verdict {
	for _, r := range c {
		if v := r.Evaluate(ctx, task); !v.Allowed {
			return v
		}
	}
	return allow
}

// ---------- 限流 ----------

// RateLimitRule 全局提交速率限制（单实例令牌桶）。
// 为什么全局而非按 key/session：控制面自我保护的语义是"总量守门"；
// 按调用方细分属于后续需求，接口已预留（换 Rule 实现即可）。
type RateLimitRule struct {
	limiter *rate.Limiter
}

// NewRateLimitRule 每分钟允许的提交数（burst 同值：瞬时不超过一分钟配额）。
func NewRateLimitRule(perMin int) *RateLimitRule {
	r := rate.Limit(float64(perMin) / 60.0)
	return &RateLimitRule{limiter: rate.NewLimiter(r, perMin)}
}

func (r *RateLimitRule) Name() string { return "rate_limit" }

func (r *RateLimitRule) Evaluate(_ context.Context, _ model.Task) Verdict {
	res := r.limiter.Reserve()
	if !res.OK() {
		return Verdict{Allowed: false, Code: CodeRateLimited, Message: "submission rate limit exceeded"}
	}
	if d := res.Delay(); d > 0 {
		res.Cancel() // 未立即获准则不占用令牌，仅告知等待时长
		return Verdict{Allowed: false, Code: CodeRateLimited,
			Message: "submission rate limit exceeded", RetryAfter: d}
	}
	return allow
}

// ---------- 成本护栏 ----------

// BudgetTracker token 日预算：按实际 usage 消耗扣减（dispatcher 回报），
// 而非提交时预估——防高估压低吞吐，也防低估击穿预算。
// 自然日重置（本地时区）；单实例内存计数，多副本需换分布式实现（接口不变）。
type BudgetTracker struct {
	mu    sync.Mutex
	limit int64
	used  int64
	day   string // 当前累计归属日（YYYY-MM-DD）
}

// NewBudgetTracker 日 token 预算上限；<=0 视为不限（不装进链）。
func NewBudgetTracker(daily int64) *BudgetTracker {
	return &BudgetTracker{limit: daily, day: time.Now().Format("2006-01-02")}
}

func (b *BudgetTracker) Name() string { return "token_budget" }

// Evaluate 提交时守门：余量耗尽即拒（新任务不再进入）。nil 接收者直接放行（未启用）。
func (b *BudgetTracker) Evaluate(_ context.Context, _ model.Task) Verdict {
	if b == nil {
		return allow
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used >= b.limit {
		return Verdict{Allowed: false, Code: CodeBudgetExceeded,
			Message: "daily token budget exhausted"}
	}
	return allow
}

// Record 消耗回报（终态 usage 汇总处调用）。跨日自动清零重新累计。nil-safe。
func (b *BudgetTracker) Record(prompt, completion int64) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if today := time.Now().Format("2006-01-02"); today != b.day {
		b.day, b.used = today, 0
	}
	b.used += prompt + completion
}

// Remaining 剩余预算（看板/调试用；无限制实现返回 -1）。
func (b *BudgetTracker) Remaining() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limit - b.used
}
