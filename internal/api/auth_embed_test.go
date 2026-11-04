package api

// 嵌入形态（Mount 到宿主子路径）与内置认证组合的回归测试：
// c.FullPath() 携带挂载前缀，权限矩阵键无前缀——路径归一（routeKey）缺失时
// 所有角色（含 admin）在嵌入形态下全部 403。

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/qisanfen666/agentflow/internal/dispatch"
	"github.com/qisanfen666/agentflow/internal/observability"
	"github.com/qisanfen666/agentflow/internal/registry"
	"github.com/qisanfen666/agentflow/model"
	"github.com/qisanfen666/agentflow/runtime"
	"github.com/qisanfen666/agentflow/storage"
)

func TestAuthEmbeddedMountPrefix(t *testing.T) {
	gin.SetMode(gin.TestMode)
	agents := storage.NewMemoryAgentStore()
	tasks := storage.NewMemoryTaskStore()
	hub := dispatch.NewHub()

	// 宿主引擎 + 挂载到 /agentflow 子路径 + 启用内置认证
	host := gin.New()
	deps := Dependencies{
		Agents: agents, Tasks: tasks,
		Dispatcher: dispatch.New(agents, tasks, hub, observability.Telemetry{}, &noopRuntime{}),
		Hub:        hub,
		Idem:   storage.NewMemoryIdemStore(),
		Tools:  registry.New(storage.NewMemoryToolStore()),
		Auth:   NewAPIKeyAuth([]APIKeyEntry{{Key: "k-admin", Roles: []string{RoleAdmin}}, {Key: "k-read", Roles: []string{RoleReader}}}),
	}
	MountRoutes(host.Group("/agentflow"), deps)
	srv := httptest.NewServer(host)
	defer srv.Close()

	cases := []struct {
		name string
		key  string
		path string
		want int
	}{
		{"admin 经前缀路径放行", "k-admin", "/agentflow/api/v1/agents", 200},
		{"reader 只读放行", "k-read", "/agentflow/api/v1/agents", 200},
		{"reader 写操作 403", "k-read", "/agentflow/api/v1/agents", 403},
		{"无 key 401", "", "/agentflow/api/v1/agents", 401},
		{"探活豁免", "", "/agentflow/health", 200},
	}
	for _, tc := range cases {
		method := http.MethodGet
		if tc.name == "reader 写操作 403" {
			method = http.MethodPost
		}
		req, _ := http.NewRequest(method, srv.URL+tc.path, nil)
		if tc.key != "" {
			req.Header.Set("X-API-Key", tc.key)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s: %s %s = %d, want %d", tc.name, method, tc.path, resp.StatusCode, tc.want)
		}
	}
}

type noopRuntime struct{}

func (*noopRuntime) Type() string { return "noop" }
func (*noopRuntime) Health(_ context.Context, _ model.RuntimeSpec) error { return nil }
func (*noopRuntime) Execute(_ context.Context, _ runtime.Request) <-chan runtime.Event {
	ch := make(chan runtime.Event)
	close(ch)
	return ch
}
