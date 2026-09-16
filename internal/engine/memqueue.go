package engine

import (
	"context"
	"sync"
	"time"

	"agentflow/model"
)

var _ Queue = (*MemoryQueue)(nil)

// MemoryQueue 是 Queue 的内存实现：memory 存储模式的配套队列，兼作单测 fake。
//
// 生命周期即进程：进程死亡队列随之消失，不存在"在途但无人处理"的中间态，
// 因此 Reclaim 恒为 no-op（可靠队列语义只在跨进程的 Redis 实现里有意义）。
type MemoryQueue struct {
	mu       sync.Mutex
	ready    []model.Task
	inflight map[string]struct{}
	notify   chan struct{} // 容量 1：新任务信号，重复入队合并唤醒
}

func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		inflight: make(map[string]struct{}),
		notify:   make(chan struct{}, 1),
	}
}

func (q *MemoryQueue) Enqueue(_ context.Context, task model.Task) error {
	q.mu.Lock()
	q.ready = append(q.ready, task)
	q.mu.Unlock()
	q.signal()
	return nil
}

// Dequeue 阻塞取任务；与 Hub 相同的"先查再睡"循环，保证不漏唤醒。
func (q *MemoryQueue) Dequeue(ctx context.Context) (model.Task, error) {
	for {
		q.mu.Lock()
		if len(q.ready) > 0 {
			task := q.ready[0]
			q.ready = q.ready[1:]
			q.inflight[task.ID] = struct{}{}
			q.mu.Unlock()
			return task, nil
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return model.Task{}, ctx.Err()
		case <-q.notify:
		}
	}
}

func (q *MemoryQueue) Ack(_ context.Context, taskID string) error {
	q.mu.Lock()
	delete(q.inflight, taskID)
	q.mu.Unlock()
	return nil
}

// Nack 用调用方提供的最新任务副本退回（接口注释：入队副本是旧的，会丢 attempt 推进）。
// 不在途的 taskID 静默忽略——at-least-once 下重复 Nack 是常态，幂等防御。
func (q *MemoryQueue) Nack(_ context.Context, task model.Task, delay time.Duration) error {
	q.mu.Lock()
	_, ok := q.inflight[task.ID]
	delete(q.inflight, task.ID)
	q.mu.Unlock()
	if !ok {
		return nil
	}
	if delay <= 0 {
		return q.Enqueue(context.Background(), task)
	}
	time.AfterFunc(delay, func() {
		_ = q.Enqueue(context.Background(), task)
	})
	return nil
}

func (q *MemoryQueue) Reclaim(_ context.Context) (int, error) { return 0, nil }

func (q *MemoryQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
