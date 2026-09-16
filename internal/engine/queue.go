// Package engine 是任务执行引擎：消费队列、驱动 dispatcher、落地重试语义。
// 本包定义自己消费的队列接口（"接口由消费者定义"的 Go 惯例，不再有集中式 interface.go）。
// 实现可以是内存的（单测 fake）或 Redis 的（BLMOVE 可靠队列）。
package engine

import (
	"context"
	"time"

	"github.com/qisanfen666/agentflow/model"
)

// Queue 是 engine 消费任务所需的队列合同（可靠队列语义）。
//
// 实现约定：
//   - Dequeue 阻塞直到有任务或 ctx 结束；取出的任务同时进入"在途"清单
//   - Ack / Nack 必须与 Dequeue 配对；Ack 后任务彻底离开队列系统
//   - 在途任务超过可见性超时未 Ack -> 被视为丢失（worker 崩溃），Reclaim 推回队列
//   - 同一任务可能被投递多次（at-least-once），消费方须以状态机终态做幂等防御
type Queue interface {
	// Enqueue 将 pending 任务入队（API 提交时调用）。
	Enqueue(ctx context.Context, task model.Task) error

	// Dequeue 阻塞取任务（worker 调用）；ctx 结束返回 ctx.Err()。
	Dequeue(ctx context.Context) (model.Task, error)

	// Ack 确认处理完成（无论成败终态已定），任务离开在途清单。
	Ack(ctx context.Context, taskID string) error

	// Nack 拒绝并退回。调用方必须传入任务的最新副本（重试路径下从存储回读，
	// 携带已推进的 attempt_count）——队列持有的入队副本是旧的。
	// delay > 0 时任务延迟后才能再次被取出（指数退避）；delay == 0 立即重回队列。
	Nack(ctx context.Context, task model.Task, delay time.Duration) error

	// Reclaim 将超过可见性超时的在途任务推回队列（worker 崩溃自愈）。
	// 由 engine 的回收协程周期调用；内存实现为 no-op（进程死亡队列随之消失，
	// 无"在途但无人处理"的中间态）。
	Reclaim(ctx context.Context) (int, error)
}
