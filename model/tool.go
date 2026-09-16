package model

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

// toolNamePattern 工具名规范：snake_case（MCP 工具名同款约定，
// 导出转换时无需改写）。
var toolNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// ToolDef 是工具的注册定义。控制面只做登记、校验与导出（MCP），
// 不实现工具本体——执行发生在 Agent 侧（payload 里声明调用，
// 结果以 tool_call / tool_result 事件回流，见 sse.md）。
type ToolDef struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`                  // 全局唯一，snake_case
	Description string         `json:"description,omitempty"` // 给 LLM 看的用途说明
	Parameters  map[string]any `json:"parameters,omitempty"`  // JSON Schema（draft 2020-12）
	TimeoutSec  int            `json:"timeout_sec,omitempty"` // 工具执行超时，0 = 用默认
	CreatedAt   time.Time      `json:"created_at"`
}

// Validate 校验必填性与命名规范。parameters 的 schema 合法性
// 由 registry 在注册时用编译器校验（模型层不引入 schema 依赖）。
func (t *ToolDef) Validate() error {
	if t.Name == "" {
		return errors.New("name is required")
	}
	if !toolNamePattern.MatchString(t.Name) {
		return fmt.Errorf("name %q must be snake_case (^[a-z][a-z0-9_]*$, <=64)", t.Name)
	}
	return nil
}

// DefaultToolTimeoutSec 工具执行默认超时（秒），对应错误分类学 TOOL_TIMEOUT 的
// 默认触发阈值。
const DefaultToolTimeoutSec = 60
