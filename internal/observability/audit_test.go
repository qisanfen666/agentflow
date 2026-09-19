package observability

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// 读回 JSONL 并逐行反序列化：任何一行非法（含交错断行）即失败。
func readAll(t *testing.T, path string) []AuditEvent {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var out []AuditEvent
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var ev AuditEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			t.Fatalf("非法审计行（可能并发交错）: %q err=%v", sc.Text(), err)
		}
		out = append(out, ev)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFileAuditLoggerRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := NewFileAuditLogger(path)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	events := []AuditEvent{
		{Action: ActionTaskSubmitted, Entity: EntityTask, EntityID: "t_1",
			Detail: map[string]any{"agent_id": "a_1", "agent_version": 2}},
		{Action: ActionTaskSucceeded, Entity: EntityTask, EntityID: "t_1"},
	}
	for _, ev := range events {
		if err := l.Record(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	got := readAll(t, path)
	if len(got) != len(events) {
		t.Fatalf("want %d events, got %d", len(events), len(got))
	}
	for i, ev := range got {
		if ev.Action != events[i].Action || ev.EntityID != events[i].EntityID {
			t.Fatalf("event %d mismatch: %+v", i, ev)
		}
		if ev.Time.IsZero() {
			t.Fatalf("logger 必须补零值 Time, got %+v", ev)
		}
	}
	if got[0].Detail["agent_version"] != float64(2) {
		t.Fatalf("detail 丢失: %+v", got[0].Detail)
	}
}

// append-only 语义：关闭后重新打开，新事件追加在旧事件之后，旧内容不变。
func TestFileAuditLoggerAppendOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	ctx := context.Background()

	l1, err := NewFileAuditLogger(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l1.Record(ctx, AuditEvent{Action: "a_first", Entity: EntityTask, EntityID: "t_1"}); err != nil {
		t.Fatal(err)
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := NewFileAuditLogger(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l2.Record(ctx, AuditEvent{Action: "a_second", Entity: EntityTask, EntityID: "t_2"}); err != nil {
		t.Fatal(err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}

	got := readAll(t, path)
	if len(got) != 2 || got[0].Action != "a_first" || got[1].Action != "a_second" {
		t.Fatalf("append 语义破坏: %+v", got)
	}
}

// 并发写 50 goroutine x 20 条：互斥锁保证行完整，总数精确。
func TestFileAuditLoggerConcurrentIntegrity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := NewFileAuditLogger(path)
	if err != nil {
		t.Fatal(err)
	}

	const workers, perWorker = 50, 20
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				RecordBestEffort(l, context.Background(), AuditEvent{
					Action: ActionTaskStarted, Entity: EntityTask,
					EntityID: "t_concurrent", Detail: map[string]any{"w": id, "i": i},
				})
			}
		}(w)
	}
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	got := readAll(t, path)
	if len(got) != workers*perWorker {
		t.Fatalf("want %d events, got %d", workers*perWorker, len(got))
	}
}

// RecordBestEffort：nil 容忍、错误不 panic。
func TestRecordBestEffortNilSafe(t *testing.T) {
	RecordBestEffort(nil, context.Background(), AuditEvent{Action: "x"}) // 不应 panic
}
