package main

import "testing"

func TestExtractConclusion(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"### Blocker\n- x\n---\nREQUEST_CHANGES", "REQUEST_CHANGES"},
		{"前面提到 REQUEST_CHANGES 的可能，但最终结论：\nAPPROVE", "APPROVE"},
		{"没有任何结论行", "UNKNOWN"},
		{"", "UNKNOWN"},
	}
	for _, c := range cases {
		if got := extractConclusion(c.in); got != c.want {
			t.Errorf("extractConclusion(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestIsFinal(t *testing.T) {
	for _, s := range []string{"succeeded", "failed", "cancelled", "timeout"} {
		if !isFinal(s) {
			t.Errorf("isFinal(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "pending", "pending_approval", "running"} {
		if isFinal(s) {
			t.Errorf("isFinal(%q) = true, want false", s)
		}
	}
}

func TestSessionID(t *testing.T) {
	p := &pr{Repo: "svc-orders", Number: 101}
	if got := sessionID(p); got != "pr-svc-orders-101" {
		t.Errorf("sessionID = %q, want pr-svc-orders-101", got)
	}
}

func TestAgentFor(t *testing.T) {
	b := &board{agentIDs: map[string]string{
		agentReviewer: "a1", agentReviewerProd: "a2", agentAutoFixer: "a3",
	}}
	if got := b.agentFor(false, ""); got != agentReviewer {
		t.Errorf("普通仓库默认 agent = %q, want %q", got, agentReviewer)
	}
	if got := b.agentFor(true, ""); got != agentReviewerProd {
		t.Errorf("生产仓库默认 agent = %q, want %q", got, agentReviewerProd)
	}
	if got := b.agentFor(false, agentAutoFixer); got != agentAutoFixer {
		t.Errorf("显式指定 = %q, want %q", got, agentAutoFixer)
	}
	if got := b.agentFor(false, "no-such-agent"); got != agentReviewer {
		t.Errorf("未知 agent 应回退默认, got %q", got)
	}
}
