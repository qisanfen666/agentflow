package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/qisanfen666/agentflow/model"
)

// 编译期接口实现检查。
var (
	_ AgentStore = (*RedisAgentStore)(nil)
	_ TaskStore  = (*RedisTaskStore)(nil)
	_ IdemStore  = (*RedisIdemStore)(nil)
)

// Redis 键布局（统一 agentflow: 前缀，避免污染共用实例）：
//
//	agentflow:agent:{id}:latest    string  最新版本号
//	agentflow:agent:{id}:v:{ver}   string  AgentSpec JSON（创建后不可变）
//	agentflow:agent:{id}:deleted   string  软删除标记（删除时间戳）
//	agentflow:agents:index         set     全部 agent id
//	agentflow:task:{id}            string  Task JSON
//	agentflow:idem:{key}           string  幂等占位（taskID，带 TTL）
//
// 版本语义（agent-versioning.md）靠 Lua 保证原子性：
// "读最新版本 -> 写新版本" 是多命令操作，非原子时并发更新会分到同一版本号。
// Lua 脚本在 Redis 单线程内原子执行，是这类 read-modify-write 的标准解法。

var (
	redisCreateScript = redis.NewScript(`
-- KEYS: [latest, index, v1]
-- ARGV: [id, specJSON]
if redis.call('EXISTS', KEYS[1]) == 1 then
  return -1
end
redis.call('SET', KEYS[3], ARGV[2])
redis.call('SET', KEYS[1], '1')
redis.call('SADD', KEYS[2], ARGV[1])
return 1
`)

	redisUpdateScript = redis.NewScript(`
-- KEYS: [latest, deleted, vNext]
-- ARGV: [baseVersion, specJSON]
if redis.call('EXISTS', KEYS[2]) == 1 then
  return -3
end
local latest = redis.call('GET', KEYS[1])
if not latest then
  return -1
end
if tonumber(latest) ~= tonumber(ARGV[1]) then
  return -2
end
local nv = latest + 1
redis.call('SET', KEYS[3], ARGV[2])
redis.call('SET', KEYS[1], nv)
return nv
`)

	redisDeleteScript = redis.NewScript(`
-- KEYS: [latest, deleted]
-- ARGV: [timestamp]
if redis.call('EXISTS', KEYS[2]) == 1 then
  return -1
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
redis.call('SET', KEYS[2], ARGV[1])
return 1
`)

	redisSaveTaskScript = redis.NewScript(`
-- KEYS: [taskKey]
-- ARGV: [taskJSON]
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1])
return 1
`)
)

// RedisAgentStore 是 AgentStore 的 Redis 实现（持久化、跨进程）。
type RedisAgentStore struct {
	client redis.UniversalClient
}

func NewRedisAgentStore(client redis.UniversalClient) *RedisAgentStore {
	return &RedisAgentStore{client: client}
}

func agentKeys(id string) (latest, deleted, index string) {
	return "agentflow:agent:" + id + ":latest",
		"agentflow:agent:" + id + ":deleted",
		"agentflow:agents:index"
}

func versionKey(id string, ver int) string {
	return "agentflow:agent:" + id + ":v:" + strconv.Itoa(ver)
}

func (s *RedisAgentStore) Create(ctx context.Context, spec model.AgentSpec) (model.AgentSpec, error) {
	if err := spec.Validate(); err != nil {
		return model.AgentSpec{}, err
	}
	spec.ID = newID("a")
	spec.Version = 1
	spec.CreatedAt = time.Now()
	data, err := json.Marshal(spec)
	if err != nil {
		return model.AgentSpec{}, err
	}
	latest, _, index := agentKeys(spec.ID)
	res, err := redisCreateScript.Run(ctx, s.client, []string{latest, index, versionKey(spec.ID, 1)},
		spec.ID, string(data)).Int()
	if err != nil {
		return model.AgentSpec{}, err
	}
	if res < 0 {
		return model.AgentSpec{}, fmt.Errorf("github.com/qisanfen666/agentflow/storage: id collision on %s", spec.ID)
	}
	return spec, nil
}

func (s *RedisAgentStore) Get(ctx context.Context, id string) (model.AgentSpec, error) {
	latestK, deletedK, _ := agentKeys(id)
	latest, err := s.client.Get(ctx, latestK).Int()
	if err == redis.Nil {
		return model.AgentSpec{}, ErrNotFound
	}
	if err != nil {
		return model.AgentSpec{}, err
	}
	if exists, _ := s.client.Exists(ctx, deletedK).Result(); exists == 1 {
		return model.AgentSpec{}, ErrNotFound
	}
	return s.GetVersion(ctx, id, latest)
}

func (s *RedisAgentStore) GetVersion(ctx context.Context, id string, version int) (model.AgentSpec, error) {
	data, err := s.client.Get(ctx, versionKey(id, version)).Bytes()
	if err == redis.Nil {
		return model.AgentSpec{}, ErrNotFound
	}
	if err != nil {
		return model.AgentSpec{}, err
	}
	var spec model.AgentSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return model.AgentSpec{}, err
	}
	return spec, nil
}

func (s *RedisAgentStore) List(ctx context.Context) ([]model.AgentSpec, error) {
	_, _, index := agentKeys("")
	ids, err := s.client.SMembers(ctx, index).Result()
	if err != nil {
		return nil, err
	}
	out := make([]model.AgentSpec, 0, len(ids))
	for _, id := range ids {
		spec, err := s.Get(ctx, id) // 软删除的返回 ErrNotFound，自然被跳过
		if err != nil {
			if err == ErrNotFound {
				continue
			}
			return nil, err
		}
		out = append(out, spec)
	}
	return out, nil
}

func (s *RedisAgentStore) Update(ctx context.Context, id string, baseVersion int, next model.AgentSpec) (model.AgentSpec, error) {
	if err := next.Validate(); err != nil {
		return model.AgentSpec{}, err
	}
	latestK, deletedK, _ := agentKeys(id)
	next.ID = id
	next.Version = baseVersion + 1
	next.CreatedAt = time.Now()
	data, err := json.Marshal(next)
	if err != nil {
		return model.AgentSpec{}, err
	}
	res, err := redisUpdateScript.Run(ctx, s.client, []string{latestK, deletedK, versionKey(id, next.Version)},
		baseVersion, string(data)).Int()
	if err != nil {
		return model.AgentSpec{}, err
	}
	switch res {
	case -1, -3:
		return model.AgentSpec{}, ErrNotFound
	case -2:
		return model.AgentSpec{}, ErrVersionConflict
	}
	return next, nil
}

func (s *RedisAgentStore) SoftDelete(ctx context.Context, id string) error {
	latestK, deletedK, _ := agentKeys(id)
	res, err := redisDeleteScript.Run(ctx, s.client, []string{latestK, deletedK},
		strconv.FormatInt(time.Now().UnixNano(), 10)).Int()
	if err != nil {
		return err
	}
	if res < 0 {
		return ErrNotFound
	}
	return nil
}

func (s *RedisAgentStore) Versions(ctx context.Context, id string) ([]model.AgentSpec, error) {
	latestK, _, _ := agentKeys(id)
	latest, err := s.client.Get(ctx, latestK).Int()
	if err == redis.Nil {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	out := make([]model.AgentSpec, 0, latest)
	for v := 1; v <= latest; v++ {
		spec, err := s.GetVersion(ctx, id, v)
		if err != nil {
			if err == ErrNotFound {
				continue // 理论不该出现（版本只增不删），容错跳过
			}
			return nil, err
		}
		out = append(out, spec)
	}
	return out, nil
}

// RedisTaskStore 是 TaskStore 的 Redis 实现。
type RedisTaskStore struct {
	client redis.UniversalClient
}

func NewRedisTaskStore(client redis.UniversalClient) *RedisTaskStore {
	return &RedisTaskStore{client: client}
}

func taskKey(id string) string { return "agentflow:task:" + id }

func (s *RedisTaskStore) Create(ctx context.Context, task model.Task) (model.Task, error) {
	if task.Status != model.TaskPending {
		return model.Task{}, errTaskMustBePending
	}
	task.ID = newID("t")
	task.AttemptCount = 0
	now := time.Now()
	task.CreatedAt = now
	task.UpdatedAt = now
	data, err := json.Marshal(task)
	if err != nil {
		return model.Task{}, err
	}
	if err := s.client.Set(ctx, taskKey(task.ID), data, 0).Err(); err != nil {
		return model.Task{}, err
	}
	return task, nil
}

func (s *RedisTaskStore) Get(ctx context.Context, id string) (model.Task, error) {
	data, err := s.client.Get(ctx, taskKey(id)).Bytes()
	if err == redis.Nil {
		return model.Task{}, ErrNotFound
	}
	if err != nil {
		return model.Task{}, err
	}
	var task model.Task
	if err := json.Unmarshal(data, &task); err != nil {
		return model.Task{}, err
	}
	return task, nil
}

func (s *RedisTaskStore) Save(ctx context.Context, task model.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	res, err := redisSaveTaskScript.Run(ctx, s.client, []string{taskKey(task.ID)}, string(data)).Int()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrNotFound
	}
	return nil
}

// ---------- IdemStore Redis 实现 ----------

// RedisIdemStore 基于 SETNX 的幂等存储。
// 为什么不用"先 GET 再 SET"：两步非原子，并发重复提交会双双通过检查；
// SETNX 是单命令原子操作，天然只有一个写入者成功。
type RedisIdemStore struct {
	client redis.UniversalClient
}

func NewRedisIdemStore(client redis.UniversalClient) *RedisIdemStore {
	return &RedisIdemStore{client: client}
}

func (s *RedisIdemStore) PutIfAbsent(ctx context.Context, key, taskID string, ttl time.Duration) (string, bool, error) {
	k := "agentflow:idem:" + key
	// SET key val NX [PX ttl]：不存在才写入，单命令原子
	ok, err := s.client.SetNX(ctx, k, taskID, ttl).Result()
	if err != nil {
		return "", false, err
	}
	if ok {
		return "", true, nil
	}
	existing, err := s.client.Get(ctx, k).Result()
	if err != nil {
		return "", false, err
	}
	return existing, false, nil
}

// ---------- ToolStore Redis 实现 ----------

// 键布局：
//
//	agentflow:tools        HASH  id -> ToolDef JSON
//	agentflow:tools:names  HASH  name -> id（唯一性索引）
const (
	keyTools     = "agentflow:tools"
	keyToolNames = "agentflow:tools:names"
)

// toolCreateScript 原子注册：名字索引 HSETNX 占位成功才写工具数据。
// 注意显式 return：go-redis 把无返回值的脚本执行结果当作 nil reply 错误。
var toolCreateScript = redis.NewScript(`
if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 0 then
  return 0
end
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
return 1
`)

type RedisToolStore struct {
	client redis.UniversalClient
}

func NewRedisToolStore(client redis.UniversalClient) *RedisToolStore {
	return &RedisToolStore{client: client}
}

func (s *RedisToolStore) Create(ctx context.Context, tool model.ToolDef) (model.ToolDef, error) {
	tool.ID = newID("tool")
	tool.CreatedAt = time.Now()
	data, err := json.Marshal(tool)
	if err != nil {
		return model.ToolDef{}, fmt.Errorf("marshal tool: %w", err)
	}
	ok, err := toolCreateScript.Run(ctx, s.client,
		[]string{keyToolNames, keyTools},
		tool.Name, tool.ID, string(data),
	).Int64()
	if err != nil {
		return model.ToolDef{}, err
	}
	if ok == 0 {
		return model.ToolDef{}, ErrDuplicate
	}
	return tool, nil
}

func (s *RedisToolStore) Get(ctx context.Context, id string) (model.ToolDef, error) {
	data, err := s.client.HGet(ctx, keyTools, id).Result()
	if errors.Is(err, redis.Nil) {
		return model.ToolDef{}, ErrNotFound
	}
	if err != nil {
		return model.ToolDef{}, err
	}
	var tool model.ToolDef
	if err := json.Unmarshal([]byte(data), &tool); err != nil {
		return model.ToolDef{}, fmt.Errorf("unmarshal tool %s: %w", id, err)
	}
	return tool, nil
}

func (s *RedisToolStore) GetByName(ctx context.Context, name string) (model.ToolDef, error) {
	id, err := s.client.HGet(ctx, keyToolNames, name).Result()
	if errors.Is(err, redis.Nil) {
		return model.ToolDef{}, ErrNotFound
	}
	if err != nil {
		return model.ToolDef{}, err
	}
	return s.Get(ctx, id)
}

func (s *RedisToolStore) List(ctx context.Context) ([]model.ToolDef, error) {
	all, err := s.client.HGetAll(ctx, keyTools).Result()
	if err != nil {
		return nil, err
	}
	out := make([]model.ToolDef, 0, len(all))
	for id, data := range all {
		var tool model.ToolDef
		if err := json.Unmarshal([]byte(data), &tool); err != nil {
			return nil, fmt.Errorf("unmarshal tool %s: %w", id, err)
		}
		out = append(out, tool)
	}
	return out, nil
}

// 编译期接口实现自检。
var _ ToolStore = (*RedisToolStore)(nil)
