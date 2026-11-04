package model

import "testing"

// TestAgentSpecValidateRuntimeType Validate 对 runtime.type 的分级语义：
// 内置类型带字段约束；自定义类型（WithRuntime 注入）只要求非空——
// 是否已注册由 API 层对注册集合校验，model 层不知道注册表。
func TestAgentSpecValidateRuntimeType(t *testing.T) {
	cases := []struct {
		name    string
		rt      RuntimeSpec
		wantErr bool
	}{
		{"python-http 缺 host", RuntimeSpec{Type: RuntimePythonHTTP}, true},
		{"python-http 齐", RuntimeSpec{Type: RuntimePythonHTTP, Host: "http://x"}, false},
		{"docker 无需 host", RuntimeSpec{Type: RuntimeDocker}, false},
		{"自定义类型放行", RuntimeSpec{Type: "claude-code"}, false},
		{"空类型报错", RuntimeSpec{}, true},
	}
	for _, tc := range cases {
		spec := AgentSpec{Name: "n", Type: "chat", Runtime: tc.rt}
		err := spec.Validate()
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
	}
}
