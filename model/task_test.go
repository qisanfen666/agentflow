package model

import "testing"

// expected 合法迁移表，与 docs/contracts/task-state.md 第 2 节一一对应。
// 文档改了这张表不改（或反之），本测试即失败——文档与代码的一致性由测试守护。
var expected = map[TaskStatus]map[TaskStatus]bool{
	TaskPending: {TaskRunning: true, TaskCancelled: true},
	TaskRunning: {
		TaskSucceeded: true,
		TaskFailed:    true,
		TaskTimeout:   true,
		TaskPending:   true, // 可重试失败，重新入队
		TaskCancelled: true,
	},
	TaskSucceeded: {},
	TaskFailed:    {},
	TaskCancelled: {},
	TaskTimeout:   {},
}

func TestCanTransitionTable(t *testing.T) {
	statuses := []TaskStatus{TaskPending, TaskRunning, TaskSucceeded, TaskFailed, TaskCancelled, TaskTimeout}
	for _, from := range statuses {
		for _, to := range statuses {
			want := expected[from][to]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s -> %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestFinalStates(t *testing.T) {
	for _, s := range []TaskStatus{TaskSucceeded, TaskFailed, TaskCancelled, TaskTimeout} {
		if !s.Final() {
			t.Errorf("%s should be final", s)
		}
	}
	for _, s := range []TaskStatus{TaskPending, TaskRunning} {
		if s.Final() {
			t.Errorf("%s should not be final", s)
		}
	}
}

func TestTransitionRejectsInvalid(t *testing.T) {
	task := &Task{ID: "t_x", Status: TaskSucceeded}
	if err := task.Transition(TaskRunning); err == nil {
		t.Fatal("transition from final state should be rejected")
	}
	if task.Status != TaskSucceeded {
		t.Fatalf("status must stay unchanged on rejection, got %s", task.Status)
	}
}

func TestTransitionAttemptCount(t *testing.T) {
	task := &Task{ID: "t_x", Status: TaskPending}

	// 首次执行：attempt 0 -> 1
	if err := task.Transition(TaskRunning); err != nil {
		t.Fatal(err)
	}
	if task.AttemptCount != 1 {
		t.Fatalf("first run attempt = %d, want 1", task.AttemptCount)
	}

	// 可重试失败重新入队：attempt 不变
	if err := task.Transition(TaskPending); err != nil {
		t.Fatal(err)
	}
	if task.AttemptCount != 1 {
		t.Fatalf("requeue attempt = %d, want 1", task.AttemptCount)
	}

	// 第二次执行：attempt -> 2
	if err := task.Transition(TaskRunning); err != nil {
		t.Fatal(err)
	}
	if task.AttemptCount != 2 {
		t.Fatalf("second run attempt = %d, want 2", task.AttemptCount)
	}

	if err := task.Transition(TaskSucceeded); err != nil {
		t.Fatal(err)
	}
}
