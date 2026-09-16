package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/qisanfen666/agentflow/model"
)

// 编译期接口实现检查。
var (
	_ AgentStore = (*MemoryAgentStore)(nil)
	_ TaskStore  = (*MemoryTaskStore)(nil)
	_ IdemStore  = (*MemoryIdemStore)(nil)
)

// errTaskMustBePending Create 只接受 pending 状态的新任务。
var errTaskMustBePending = errors.New("github.com/qisanfen666/agentflow/storage: task must be pending on create")

// newID 生成带前缀的短随机 ID，如 "a_3f9a2c1d"。
// 4 字节 crypto/rand = 8 个十六进制字符，演示场景碰撞概率足够低；
// 不用 UUID：太长且日志难读；不上雪花：单机用不上（过度设计）。
func newID(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

// ---------- AgentStore 内存实现 ----------

// agentRecord 一个 Agent 的存储单元：全部历史版本 + 软删除标记。
// 版本列表追加式不可变，deletedAt 独立于版本，保证"版本不可变"语义不被破坏。
type agentRecord struct {
	versions  []model.AgentSpec // 按版本号升序，末尾即最新
	deletedAt *time.Time
}

func (r *agentRecord) latest() model.AgentSpec { return r.versions[len(r.versions)-1] }

// MemoryAgentStore 是 AgentStore 的内存实现（默认驱动，兼作测试 fake）。
// 数据不持久化，仅用于开发/演示/测试。
type MemoryAgentStore struct {
	mu     sync.RWMutex
	agents map[string]*agentRecord
}

// NewMemoryAgentStore 创建空的内存 Agent 存储。
func NewMemoryAgentStore() *MemoryAgentStore {
	return &MemoryAgentStore{agents: make(map[string]*agentRecord)}
}

func (s *MemoryAgentStore) Create(_ context.Context, spec model.AgentSpec) (model.AgentSpec, error) {
	if err := spec.Validate(); err != nil {
		return model.AgentSpec{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	spec.ID = newID("a")
	spec.Version = 1
	spec.CreatedAt = time.Now()
	s.agents[spec.ID] = &agentRecord{versions: []model.AgentSpec{spec}}
	return spec, nil
}

func (s *MemoryAgentStore) Get(_ context.Context, id string) (model.AgentSpec, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.agents[id]
	if !ok || rec.deletedAt != nil {
		return model.AgentSpec{}, ErrNotFound
	}
	return rec.latest(), nil
}

func (s *MemoryAgentStore) GetVersion(_ context.Context, id string, version int) (model.AgentSpec, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.agents[id]
	if !ok {
		return model.AgentSpec{}, ErrNotFound
	}
	// 历史版本不因软删除而消失：版本锁定任务仍按锁定版本执行
	for _, v := range rec.versions {
		if v.Version == version {
			return v, nil
		}
	}
	return model.AgentSpec{}, ErrNotFound
}

func (s *MemoryAgentStore) List(_ context.Context) ([]model.AgentSpec, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.AgentSpec, 0, len(s.agents))
	for _, rec := range s.agents {
		if rec.deletedAt != nil {
			continue
		}
		out = append(out, rec.latest())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *MemoryAgentStore) Update(_ context.Context, id string, baseVersion int, next model.AgentSpec) (model.AgentSpec, error) {
	if err := next.Validate(); err != nil {
		return model.AgentSpec{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.agents[id]
	if !ok || rec.deletedAt != nil {
		return model.AgentSpec{}, ErrNotFound
	}
	// 乐观锁：baseVersion 必须等于当前最新版本，否则有并发更新先落了
	if latest := rec.latest(); latest.Version != baseVersion {
		return model.AgentSpec{}, ErrVersionConflict
	}
	next.ID = id
	next.Version = baseVersion + 1
	next.CreatedAt = time.Now()
	rec.versions = append(rec.versions, next)
	return next, nil
}

func (s *MemoryAgentStore) SoftDelete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.agents[id]
	if !ok || rec.deletedAt != nil {
		return ErrNotFound
	}
	now := time.Now()
	rec.deletedAt = &now
	return nil
}

func (s *MemoryAgentStore) Versions(_ context.Context, id string) ([]model.AgentSpec, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.agents[id]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]model.AgentSpec, len(rec.versions))
	copy(out, rec.versions)
	return out, nil
}

// ---------- TaskStore 内存实现 ----------

// MemoryTaskStore 是 TaskStore 的内存实现。
type MemoryTaskStore struct {
	mu    sync.RWMutex
	tasks map[string]model.Task
}

// NewMemoryTaskStore 创建空的内存 Task 存储。
func NewMemoryTaskStore() *MemoryTaskStore {
	return &MemoryTaskStore{tasks: make(map[string]model.Task)}
}

func (s *MemoryTaskStore) Create(_ context.Context, task model.Task) (model.Task, error) {
	if task.Status != model.TaskPending {
		return model.Task{}, errTaskMustBePending
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task.ID = newID("t")
	task.AttemptCount = 0
	now := time.Now()
	task.CreatedAt = now
	task.UpdatedAt = now
	s.tasks[task.ID] = task
	return task, nil
}

func (s *MemoryTaskStore) Get(_ context.Context, id string) (model.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, ok := s.tasks[id]
	if !ok {
		return model.Task{}, ErrNotFound
	}
	return task, nil
}

func (s *MemoryTaskStore) Save(_ context.Context, task model.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[task.ID]; !ok {
		return ErrNotFound
	}
	s.tasks[task.ID] = task
	return nil
}

// ---------- IdemStore 内存实现 ----------

type idemEntry struct {
	taskID    string
	expiresAt time.Time // zero = 永不过期
}

// MemoryIdemStore 内存版幂等存储，带惰性过期清理。
type MemoryIdemStore struct {
	mu sync.Mutex
	m  map[string]idemEntry
}

func NewMemoryIdemStore() *MemoryIdemStore {
	return &MemoryIdemStore{m: make(map[string]idemEntry)}
}

func (s *MemoryIdemStore) PutIfAbsent(_ context.Context, key, taskID string, ttl time.Duration) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.m[key]; ok {
		if e.expiresAt.IsZero() || time.Now().Before(e.expiresAt) {
			return e.taskID, false, nil
		}
		delete(s.m, key) // 惰性清理过期项
	}
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	s.m[key] = idemEntry{taskID: taskID, expiresAt: exp}
	return "", true, nil
}

// ---------- ToolStore 内存实现 ----------

// MemoryToolStore 双索引（id / name）内存注册表，锁内保证名字唯一性判定原子。
type MemoryToolStore struct {
	mu     sync.Mutex
	byID   map[string]model.ToolDef
	byName map[string]string // name -> id
}

func NewMemoryToolStore() *MemoryToolStore {
	return &MemoryToolStore{
		byID:   make(map[string]model.ToolDef),
		byName: make(map[string]string),
	}
}

func (s *MemoryToolStore) Create(_ context.Context, tool model.ToolDef) (model.ToolDef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.byName[tool.Name]; dup {
		return model.ToolDef{}, ErrDuplicate
	}
	tool.ID = newID("tool")
	tool.CreatedAt = time.Now()
	s.byID[tool.ID] = tool
	s.byName[tool.Name] = tool.ID
	return tool, nil
}

func (s *MemoryToolStore) Get(_ context.Context, id string) (model.ToolDef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tool, ok := s.byID[id]
	if !ok {
		return model.ToolDef{}, ErrNotFound
	}
	return tool, nil
}

func (s *MemoryToolStore) GetByName(_ context.Context, name string) (model.ToolDef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byName[name]
	if !ok {
		return model.ToolDef{}, ErrNotFound
	}
	return s.byID[id], nil
}

func (s *MemoryToolStore) List(_ context.Context) ([]model.ToolDef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.ToolDef, 0, len(s.byID))
	for _, t := range s.byID {
		out = append(out, t)
	}
	return out, nil
}

// 编译期接口实现自检。
var _ ToolStore = (*MemoryToolStore)(nil)
