// Package registry 是工具注册表：登记时用 JSON Schema 编译器校验参数定义，
// 并提供 MCP 标准格式（tools/list 的 tool 对象）的导出转换。
// 控制面只管"定义合法、可被发现"，工具执行发生在 Agent 侧。
package registry

import (
	"context"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/qisanfen666/agentflow/model"
	"github.com/qisanfen666/agentflow/storage"
)

// Registry 组合存储与校验。薄层：存储语义全部委托 ToolStore。
type Registry struct {
	store storage.ToolStore
}

func New(store storage.ToolStore) *Registry {
	return &Registry{store: store}
}

// Register 登记工具：命名校验 -> schema 编译校验 -> 存储唯一性。
// schema 编译不通过说明定义本身写错了，直接失败（上层映射 400）。
func (r *Registry) Register(ctx context.Context, tool model.ToolDef) (model.ToolDef, error) {
	if err := tool.Validate(); err != nil {
		return model.ToolDef{}, err
	}
	if tool.Parameters != nil {
		if _, err := compileSchema(tool.Parameters); err != nil {
			return model.ToolDef{}, fmt.Errorf("invalid parameters schema: %w", err)
		}
	}
	if tool.TimeoutSec == 0 {
		tool.TimeoutSec = model.DefaultToolTimeoutSec
	}
	return r.store.Create(ctx, tool)
}

// ValidateArgs 校验一次工具调用的实参是否符合定义的 schema。
// 供上层做调用前校验（后续治理阶段将接入调用链）。
func (r *Registry) ValidateArgs(tool model.ToolDef, args map[string]any) error {
	if tool.Parameters == nil {
		return nil // 无 schema 约束
	}
	sch, err := compileSchema(tool.Parameters)
	if err != nil {
		return fmt.Errorf("registered schema no longer compiles: %w", err)
	}
	return sch.Validate(args)
}

func (r *Registry) Get(ctx context.Context, id string) (model.ToolDef, error) {
	return r.store.Get(ctx, id)
}

func (r *Registry) List(ctx context.Context) ([]model.ToolDef, error) {
	return r.store.List(ctx)
}

// compileSchema 编译一份 JSON Schema（无 $schema 声明时按最新 draft，2020-12）。
// v6 API 注意：AddResource 的第二参是已解析的 Go 值（不是 io.Reader）。
// 编译开销可控且定义不可变，不做缓存（量级是配置不是流量）。
func compileSchema(m map[string]any) (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	if err := c.AddResource("tool-schema.json", m); err != nil {
		return nil, err
	}
	return c.Compile("tool-schema.json")
}

// ---------- MCP 导出 ----------

// MCPTool 是 MCP tools/list 里单个 tool 对象的标准形态
// （与 OpenAI function-calling 的 tools 结构同构，Agent 侧零适配可消费）。
type MCPTool struct {
	Type     string      `json:"type"` // 固定 "function"
	Function MCPFunction `json:"function"`
}

type MCPFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"` // JSON Schema 原样透传
}

// ToMCP 单个工具转 MCP 格式。
func ToMCP(tool model.ToolDef) MCPTool {
	return MCPTool{
		Type: "function",
		Function: MCPFunction{
			Name:        tool.Name,
			Description: tool.Description,
			Parameters:  tool.Parameters,
		},
	}
}

// ListMCP 全量导出（GET /tools/mcp 的响应体）。
func (r *Registry) ListMCP(ctx context.Context) ([]MCPTool, error) {
	tools, err := r.store.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]MCPTool, 0, len(tools))
	for _, t := range tools {
		out = append(out, ToMCP(t))
	}
	return out, nil
}
