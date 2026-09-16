package storage

import "testing"

// memory 实现的合同测试接线：全部语义断言在 contract_test.go 的共享套件里。

func memoryFactory(t *testing.T) (AgentStore, TaskStore) {
	t.Helper()
	return NewMemoryAgentStore(), NewMemoryTaskStore()
}

func TestMemoryAgentLifecycle(t *testing.T) { runAgentLifecycle(t, memoryFactory) }

func TestMemoryOptimisticLock(t *testing.T) { runOptimisticLock(t, memoryFactory) }

func TestMemoryConcurrentUpdateAtomicity(t *testing.T) {
	runConcurrentUpdateAtomicity(t, memoryFactory)
}

func TestMemoryTaskBasics(t *testing.T) { runTaskBasics(t, memoryFactory) }

func TestMemoryIdem(t *testing.T) {
	runIdemSemantics(t, func(t *testing.T) IdemStore { return NewMemoryIdemStore() })
}

func TestMemoryTools(t *testing.T) {
	runToolSemantics(t, func(t *testing.T) ToolStore { return NewMemoryToolStore() })
}
