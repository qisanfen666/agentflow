package engine

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"agentflow/internal/dispatch"
	"agentflow/model"
	"agentflow/storage"
)

// WorkerConfig 消费行为参数（wiring 时映射自 config.QueueConfig）。
type WorkerConfig struct {
	Concurrency      int           // 并发消费协程数，默认 1
	MaxRetries       int           // 重试上限，<=0 时用 dispatch.DefaultMaxRetries
	RetryBackoffBase time.Duration // 退避基数：delay = base * attempt^2，默认 1s
	ReclaimInterval  time.Duration // 可见性回收周期，默认 30s
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = dispatch.DefaultMaxRetries
	}
	if c.RetryBackoffBase <= 0 {
		c.RetryBackoffBase = time.Second
	}
	if c.ReclaimInterval <= 0 {
		c.ReclaimInterval = 30 * time.Second
	}
	return c
}

// Worker 是队列消费循环：Dequeue -> 归一化状态 -> dispatcher.Execute -> Ack/Nack。
// 它是"队列世界"与"执行世界"的桥——重试决策在这里落地。
type Worker struct {
	queue Queue
	disp  *dispatch.Dispatcher
	tasks storage.TaskStore
	cfg   WorkerConfig
}

func NewWorker(queue Queue, disp *dispatch.Dispatcher, tasks storage.TaskStore, cfg WorkerConfig) *Worker {
	cfg = cfg.withDefaults()
	disp.MaxRetries = cfg.MaxRetries // 重试上限统一由 worker 配置驱动
	return &Worker{queue: queue, disp: disp, tasks: tasks, cfg: cfg}
}

// Start 启动 Concurrency 个消费协程 + 1 个回收协程，返回停止函数（优雅关停用）。
func (w *Worker) Start(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup

	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Run(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(w.cfg.ReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if n, err := w.queue.Reclaim(ctx); err != nil {
					log.Printf("[engine] reclaim: %v", err)
				} else if n > 0 {
					log.Printf("[engine] reclaimed %d stuck task(s)", n)
				}
			}
		}
	}()

	return func() {
		cancel()
		wg.Wait()
	}
}

// Run 阻塞消费直到 ctx 结束。每轮消费独立容错：单任务故障不拖垮循环。
func (w *Worker) Run(ctx context.Context) {
	for {
		task, err := w.queue.Dequeue(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // 正常关停
			}
			log.Printf("[engine] dequeue: %v", err)
			time.Sleep(100 * time.Millisecond) // 队列故障退避，防热循环
			continue
		}
		w.handle(ctx, task)
	}
}

func (w *Worker) handle(ctx context.Context, task model.Task) {
	// 归一化：running = 上一个消费者带着任务崩溃了（可见性超时重投递）。
	// 回退 pending 让 Execute 能重新 pending->running；attempt 已计入，不重置。
	if task.Status == model.TaskRunning {
		if err := task.Transition(model.TaskPending); err == nil {
			if err := w.tasks.Save(ctx, task); err != nil {
				log.Printf("[engine] normalize %s: %v", task.ID, err)
			}
		}
	}

	err := w.disp.Execute(task)

	var re *dispatch.RetryableError
	switch {
	case err == nil:
		// 终态已定（含 succeeded/failed/timeout/cancelled）
		if err := w.queue.Ack(ctx, task.ID); err != nil {
			log.Printf("[engine] ack %s: %v", task.ID, err)
		}
	case errors.As(err, &re):
		// 回读存储拿最新副本（attempt 已推进、状态已回 pending），
		// 避免拿 Dequeue 时的旧副本重投导致 attempt 永不推进（无限重试）
		fresh, gerr := w.tasks.Get(ctx, task.ID)
		if gerr != nil {
			fresh = task // 存储读失败兜底：旧副本 + 状态归一化仍可自愈
		}
		delay := w.backoff(re.Attempt)
		if err := w.queue.Nack(ctx, fresh, delay); err != nil {
			log.Printf("[engine] nack %s: %v", task.ID, err)
		}
		log.Printf("[engine] task %s retry in %v (attempt %d, %s)", task.ID, delay, re.Attempt, re.Code)
	default:
		// 内部故障（存储异常等）：状态未定，退避后重投；
		// 用固定退避防热循环（此时 attempt 可能未推进）。
		if err := w.queue.Nack(ctx, task, w.cfg.RetryBackoffBase); err != nil {
			log.Printf("[engine] nack %s: %v", task.ID, err)
		}
		log.Printf("[engine] task %s internal error, requeued: %v", task.ID, err)
	}
}

// backoff 指数退避：delay = base * attempt^2（合同 task-state.md 第 3 节）。
// attempt=1 -> 1x, 2 -> 4x, 3 -> 9x；重试上限默认 3，不会长到离谱。
func (w *Worker) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return w.cfg.RetryBackoffBase * time.Duration(attempt*attempt)
}
