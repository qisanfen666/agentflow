package registry

import (
	"context"
	"errors"
	"testing"

	"agentflow/model"
	"agentflow/storage"
)

func newRegistry() *Registry {
	return New(storage.NewMemoryToolStore())
}

func validTool() model.ToolDef {
	return model.ToolDef{
		Name:        "search_docs",
		Description: "检索文档",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string", "minLength": 1},
				"top_k": map[string]any{"type": "integer", "minimum": 1},
			},
			"required": []any{"query"},
		},
	}
}

// TestRegisterSchemaRejected schema 本身非法 -> 拒绝登记。
func TestRegisterSchemaRejected(t *testing.T) {
	r := newRegistry()
	tool := validTool()
	tool.Parameters = map[string]any{"type": "nonsense_type"}

	if _, err := r.Register(context.Background(), tool); err == nil {
		t.Fatal("invalid schema must be rejected")
	}
}

// TestRegisterBadName 命名违规 -> 拒绝。
func TestRegisterBadName(t *testing.T) {
	r := newRegistry()
	tool := validTool()
	tool.Name = "Bad-Name"

	if _, err := r.Register(context.Background(), tool); err == nil {
		t.Fatal("bad name must be rejected")
	}
}

// TestRegisterDefaults 超时缺省填充。
func TestRegisterDefaults(t *testing.T) {
	r := newRegistry()
	tool, err := r.Register(context.Background(), validTool())
	if err != nil {
		t.Fatal(err)
	}
	if tool.TimeoutSec != model.DefaultToolTimeoutSec {
		t.Fatalf("timeout = %d, want default %d", tool.TimeoutSec, model.DefaultToolTimeoutSec)
	}
}

// TestValidateArgs 实参校验：合法通过 / 缺必填拒绝 / 类型错拒绝。
func TestValidateArgs(t *testing.T) {
	r := newRegistry()
	tool, err := r.Register(context.Background(), validTool())
	if err != nil {
		t.Fatal(err)
	}

	if err := r.ValidateArgs(tool, map[string]any{"query": "agentflow", "top_k": 3}); err != nil {
		t.Fatalf("valid args rejected: %v", err)
	}
	if err := r.ValidateArgs(tool, map[string]any{"top_k": 3}); err == nil {
		t.Fatal("missing required 'query' must be rejected")
	}
	if err := r.ValidateArgs(tool, map[string]any{"query": "x", "top_k": "three"}); err == nil {
		t.Fatal("wrong type must be rejected")
	}
}

// TestDuplicate 名字重复透传存储层哨兵。
func TestDuplicate(t *testing.T) {
	r := newRegistry()
	if _, err := r.Register(context.Background(), validTool()); err != nil {
		t.Fatal(err)
	}
	_, err := r.Register(context.Background(), validTool())
	if !errors.Is(err, storage.ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
}

// TestToMCP MCP 导出形态。
func TestToMCP(t *testing.T) {
	r := newRegistry()
	_, err := r.Register(context.Background(), validTool())
	if err != nil {
		t.Fatal(err)
	}

	mcpTools, err := r.ListMCP(context.Background())
	if err != nil || len(mcpTools) != 1 {
		t.Fatalf("ListMCP = %v err=%v", mcpTools, err)
	}
	mt := mcpTools[0]
	if mt.Type != "function" || mt.Function.Name != "search_docs" {
		t.Fatalf("mcp shape wrong: %+v", mt)
	}
	if mt.Function.Parameters == nil || mt.Function.Parameters["type"] != "object" {
		t.Fatalf("parameters must pass through: %+v", mt.Function.Parameters)
	}
	if mt.Function.Description != "检索文档" {
		t.Fatalf("description lost: %q", mt.Function.Description)
	}
}
