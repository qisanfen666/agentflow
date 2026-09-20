package policy

import (
	"context"
	"testing"

	"github.com/qisanfen666/agentflow/model"
)

var dummy = model.Task{Status: model.TaskPending, AgentID: "a_1"}

func TestRateLimitBurstThenDeny(t *testing.T) {
	r := NewRateLimitRule(2) // 每分钟 2，burst 2
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if v := r.Evaluate(ctx, dummy); !v.Allowed {
			t.Fatalf("submission %d should pass, got %+v", i, v)
		}
	}
	v := r.Evaluate(ctx, dummy)
	if v.Allowed || v.Code != CodeRateLimited {
		t.Fatalf("3rd submission should be denied, got %+v", v)
	}
	if v.RetryAfter <= 0 {
		t.Fatalf("denied verdict should carry RetryAfter, got %v", v.RetryAfter)
	}
}

func TestBudgetExhaustedThenReset(t *testing.T) {
	b := NewBudgetTracker(100)
	ctx := context.Background()

	if v := b.Evaluate(ctx, dummy); !v.Allowed {
		t.Fatalf("fresh budget should allow, got %+v", v)
	}
	b.Record(60, 40) // 消耗 100 = 预算
	if rem := b.Remaining(); rem != 0 {
		t.Fatalf("remaining = %d, want 0", rem)
	}
	if v := b.Evaluate(ctx, dummy); v.Allowed || v.Code != CodeBudgetExceeded {
		t.Fatalf("exhausted budget should deny, got %+v", v)
	}

	// 跨日重置：改归属日后余量恢复
	b.mu.Lock()
	b.day = "2000-01-01"
	b.mu.Unlock()
	b.Record(0, 0) // 任意一次回报触发日界检查
	if rem := b.Remaining(); rem != b.limit {
		t.Fatalf("after day rollover remaining = %d, want %d", rem, b.limit)
	}
	if v := b.Evaluate(ctx, dummy); !v.Allowed {
		t.Fatalf("after rollover should allow, got %+v", v)
	}
}

func TestChainShortCircuit(t *testing.T) {
	denier := &fixedRule{name: "denier", verdict: Verdict{Allowed: false, Code: "X", Message: "no"}}
	counter := &countingRule{}
	chain := Chain{denier, counter}

	if v := chain.Evaluate(context.Background(), dummy); v.Allowed || v.Code != "X" {
		t.Fatalf("chain should return first denial, got %+v", v)
	}
	if counter.calls != 0 {
		t.Fatal("chain must short-circuit after first denial")
	}
}

type fixedRule struct {
	name    string
	verdict Verdict
}

func (f *fixedRule) Name() string                                 { return f.name }
func (f *fixedRule) Evaluate(context.Context, model.Task) Verdict { return f.verdict }

type countingRule struct{ calls int }

func (c *countingRule) Name() string                                 { return "counter" }
func (c *countingRule) Evaluate(context.Context, model.Task) Verdict { c.calls++; return allow }
