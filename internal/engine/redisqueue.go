package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/qisanfen666/agentflow/model"
)

var _ Queue = (*RedisQueue)(nil)

// Redis 队列键布局（跨进程共享，前缀 agentflow:q:）：
//
//	ready      LIST  待消费 taskID（LPUSH 生产 / BLMOVE 从右侧取最旧，FIFO）
//	processing LIST  在途 taskID（取出即入；Ack/Nack/Reclaim 移除）
//	delayed    ZSET  member=taskID, score=就绪时刻(ms)——指数退避的延迟队列
//	tasks      HASH  field=taskID, value=Task JSON（队列条目正身，Ack 时删除）
//	seen:{id}  STRING EX=可见性超时——在途租约：存在=有 worker 正在处理，
//	           过期=worker 已崩溃，Reclaim 把它推回 ready
//
// 为什么 tasks 单独放 HASH 而不是把 JSON 直接塞进 ready：
// Nack 需要写入"最新副本"（attempt 已推进），而 processing 里躺着的还是旧 JSON；
// ID 与数据分离后，移动的永远是 ID，数据原地更新。
const (
	keyReady      = "agentflow:q:ready"
	keyProcessing = "agentflow:q:processing"
	keyDelayed    = "agentflow:q:delayed"
	keyTasks      = "agentflow:q:tasks"
	keySeenFmt    = "agentflow:q:seen:%s"
)

// claimScript 取出任务的配套动作：登记在途租约 + 读任务正身。
// KEYS[1]=tasks hash, KEYS[2]=seen key；ARGV[1]=id, ARGV[2]=可见性秒数。
var claimScript = redis.NewScript(`
redis.call('SET', KEYS[2], '1', 'EX', tonumber(ARGV[2]))
return redis.call('HGET', KEYS[1], ARGV[1])
`)

// ackScript 结算：移出在途 + 删正身 + 删租约（三步原子）。
// 注意 Lua 必须显式 return 非空值，否则 go-redis 收到 nil reply 会当错误。
var ackScript = redis.NewScript(`
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[3])
return 1
`)

// nackScript 退回：更新正身为最新副本，移出在途，按 flag 进 ready 或 delayed。
// LREM 返回 0 = 不在途（重复 Nack / 已被回收），跳过移动——幂等防御。
// KEYS[1]=processing, KEYS[2]=tasks hash, KEYS[3]=ready, KEYS[4]=delayed, KEYS[5]=seen
// ARGV[1]=id, ARGV[2]=task JSON, ARGV[3]=0(立即)/1(延迟), ARGV[4]=就绪时刻 ms
var nackScript = redis.NewScript(`
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('DEL', KEYS[5])
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
if ARGV[3] == '1' then
  redis.call('ZADD', KEYS[4], ARGV[4], ARGV[1])
else
  redis.call('LPUSH', KEYS[3], ARGV[1])
end
return 1
`)

// RedisQueue 是 Queue 的 Redis 实现：BLMOVE 可靠队列 + ZSET 延迟退避 + 可见性回收。
// 优先 BLMOVE（Redis >= 6.2）；遇到不识别该命令的老实例自动降级为等价的
// BRPopLPush（二者语义相同：右弹出左压入的原子移动），旧集群也能跑。
type RedisQueue struct {
	rdb        *redis.Client
	visibility time.Duration
	legacyPop  atomic.Bool // true = 已探测到老 Redis，改用 BRPopLPush
}

// NewRedisQueue visibility 为在途租约时长（<=0 用 60s）：任务处理超过它即被视为
// worker 崩溃，可被回收重投。应大于最长任务的执行时长。
func NewRedisQueue(rdb *redis.Client, visibility time.Duration) *RedisQueue {
	if visibility <= 0 {
		visibility = 60 * time.Second
	}
	return &RedisQueue{rdb: rdb, visibility: visibility}
}

// Start 启动两个后台循环：mover（delayed 到期搬回 ready，200ms 粒度）
// 和回收器（可见性超时扫描，5s 粒度）。返回停止函数。
// 回收器与 Worker.Reclaim 周期调用的是同一套幂等扫描，重复跑无害。
func (q *RedisQueue) Start(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		mover := time.NewTicker(200 * time.Millisecond)
		reclaimer := time.NewTicker(time.Second) // 1s 粒度：孤儿 BLMOVE 窃取的兜底要够快
		defer mover.Stop()
		defer reclaimer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-mover.C:
				if err := q.moveDelayed(ctx); err != nil && ctx.Err() == nil {
					log.Printf("[redisqueue] mover: %v", err)
				}
			case <-reclaimer.C:
				if _, err := q.Reclaim(ctx); err != nil && ctx.Err() == nil {
					log.Printf("[redisqueue] reclaimer: %v", err)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (q *RedisQueue) Enqueue(ctx context.Context, task model.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	pipe := q.rdb.Pipeline()
	pipe.HSet(ctx, keyTasks, task.ID, data)
	pipe.LPush(ctx, keyReady, task.ID)
	_, err = pipe.Exec(ctx)
	return err
}

// Dequeue 阻塞取任务。
// 服务端超时故意只用 1s：Go 的 ctx 取消不了服务端的 BLMOVE，被遗弃的命令
// 会继续活着并"偷走"后续入队任务（推进 processing 却不设租约）——
// 短超时让孤儿命令尽快自灭，缩小窃取窗口；被偷的任务由回收器
// （在 processing 但无租约 -> 重投）兜底，at-least-once 语义保持成立。
func (q *RedisQueue) Dequeue(ctx context.Context) (model.Task, error) {
	for {
		id, err := q.popReady(ctx)
		if err == redis.Nil {
			continue // 服务端 1s 超时无任务：回循环重试（ctx 已取消则由下行返回）
		}
		if err != nil {
			if ctx.Err() != nil {
				return model.Task{}, ctx.Err()
			}
			return model.Task{}, fmt.Errorf("blmove: %w", err)
		}

		data, err := claimScript.Run(ctx, q.rdb,
			[]string{keyTasks, fmt.Sprintf(keySeenFmt, id)},
			id, int(q.visibility.Seconds()),
		).Text()
		if errors.Is(err, redis.Nil) {
			data = "" // HGET 打空 = 幽灵条目，走下方清理
		} else if err != nil {
			return model.Task{}, fmt.Errorf("claim %s: %w", id, err)
		}
		if data == "" {
			// 幽灵条目：ID 在队列里但正身已删（Ack 后的重复投递等）——清掉继续
			q.rdb.LRem(ctx, keyProcessing, 1, id)
			continue
		}

		var task model.Task
		if err := json.Unmarshal([]byte(data), &task); err != nil {
			// 毒丸防御：删正身移出在途，绝不让坏数据卡死 worker
			_ = ackScript.Run(ctx, q.rdb,
				[]string{keyProcessing, keyTasks, fmt.Sprintf(keySeenFmt, id)}, id).Err()
			log.Printf("[redisqueue] poison task %s dropped: %v", id, err)
			continue
		}
		return task, nil
	}
}

// popReady 阻塞把 ready 队头的 taskID 原子移入 processing。
// BLMOVE 不被识别（老 Redis）时置 legacyPop，全程退回 BRPopLPush。
func (q *RedisQueue) popReady(ctx context.Context) (string, error) {
	if !q.legacyPop.Load() {
		id, err := q.rdb.BLMove(ctx, keyReady, keyProcessing, "RIGHT", "LEFT", time.Second).Result()
		if err == nil || !isUnknownCommand(err) {
			return id, err
		}
		q.legacyPop.Store(true)
		log.Printf("[redisqueue] BLMOVE 不可用（老版本 Redis），降级为 BRPopLPush")
	}
	return q.rdb.BRPopLPush(ctx, keyReady, keyProcessing, time.Second).Result()
}

// isUnknownCommand 识别 Redis 的 "unknown command" 服务端错误。
// 没有结构化错误类型可用，字符串匹配是 go-redis 生态的通行做法。
func isUnknownCommand(err error) bool {
	var rerr redis.Error
	if !errors.As(err, &rerr) {
		return false
	}
	return strings.Contains(err.Error(), "unknown command")
}

func (q *RedisQueue) Ack(ctx context.Context, taskID string) error {
	return ackScript.Run(ctx, q.rdb,
		[]string{keyProcessing, keyTasks, fmt.Sprintf(keySeenFmt, taskID)},
		taskID,
	).Err()
}

func (q *RedisQueue) Nack(ctx context.Context, task model.Task, delay time.Duration) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	delayed := "0"
	readyAt := int64(0)
	if delay > 0 {
		delayed = "1"
		readyAt = time.Now().Add(delay).UnixMilli()
	}
	return nackScript.Run(ctx, q.rdb,
		[]string{keyProcessing, keyTasks, keyReady, keyDelayed, fmt.Sprintf(keySeenFmt, task.ID)},
		task.ID, string(data), delayed, readyAt,
	).Err()
}

// Reclaim 扫描在途清单：租约（seen:{id}）已过期 = worker 崩溃。
// LREM 返回计数做并发护栏——多个实例同时回收时只有一个推回成功。
func (q *RedisQueue) Reclaim(ctx context.Context) (int, error) {
	ids, err := q.rdb.LRange(ctx, keyProcessing, 0, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("lrange processing: %w", err)
	}
	n := 0
	for _, id := range ids {
		exists, err := q.rdb.Exists(ctx, fmt.Sprintf(keySeenFmt, id)).Result()
		if err != nil {
			return n, err
		}
		if exists == 1 {
			continue // 仍在租约内
		}
		removed, err := q.rdb.LRem(ctx, keyProcessing, 1, id).Result()
		if err != nil {
			return n, err
		}
		if removed > 0 {
			if err := q.rdb.LPush(ctx, keyReady, id).Err(); err != nil {
				return n, err
			}
			n++
			log.Printf("[redisqueue] reclaimed stuck task %s", id)
		}
	}
	return n, nil
}

// moveDelayed 把到期的延迟任务搬回 ready。ZREM 计数护栏防多实例重复搬运。
func (q *RedisQueue) moveDelayed(ctx context.Context) error {
	now := time.Now().UnixMilli()
	ids, err := q.rdb.ZRangeByScore(ctx, keyDelayed, &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprintf("%d", now), Count: 100,
	}).Result()
	if err != nil {
		return fmt.Errorf("zrangebyscore delayed: %w", err)
	}
	for _, id := range ids {
		removed, err := q.rdb.ZRem(ctx, keyDelayed, id).Result()
		if err != nil {
			return err
		}
		if removed > 0 {
			if err := q.rdb.LPush(ctx, keyReady, id).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}
