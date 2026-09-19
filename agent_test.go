package agentflow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

// TestNewRejectsBadConfig 非法配置在 New 即失败：未知驱动 / 未知 Runtime / 重复 Runtime。
func TestNewRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"未知存储驱动", Config{Storage: StorageConfig{Driver: "mysql"}}, "存储驱动"},
		{"未知 runtime", Config{Runtimes: []string{"firecracker"}}, "未知类型"},
		{"重复 runtime", Config{Runtimes: []string{"python-http", "python-http"}}, "重复声明"},
	}
	for _, tc := range cases {
		if _, err := New(tc.cfg); err == nil {
			t.Errorf("%s: 应当报错", tc.name)
		}
	}
}

// TestNewRedisUnreachable Redis 不可达时 New 快速失败（而非运行半天才暴露）。
func TestNewRedisUnreachable(t *testing.T) {
	cfg := Config{Storage: StorageConfig{Driver: "redis", Redis: RedisConfig{Addr: "localhost:1"}}}
	start := time.Now()
	if _, err := New(cfg); err == nil {
		t.Fatal("redis 不可达应当报错")
	}
	if elapsed := time.Since(start); elapsed > 6*time.Second {
		t.Fatalf("连通性检查过慢: %v", elapsed)
	}
}

// TestPanelLifecycle 独立形态：Start -> Router 可服务 -> Stop 幂等 -> 不可重启。
func TestPanelLifecycle(t *testing.T) {
	panel, err := New(Config{}) // 全默认：memory + python-http
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := panel.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// Start 幂等
	if err := panel.Start(ctx); err != nil {
		t.Fatalf("二次 Start 应幂等: %v", err)
	}

	srv := httptest.NewServer(panel.Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("health: %v %v", err, resp)
	}
	resp.Body.Close()

	if err := panel.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	// Stop 幂等
	if err := panel.Stop(ctx); err != nil {
		t.Fatalf("二次 Stop 应幂等: %v", err)
	}
	// 停止后不可重启
	if err := panel.Start(ctx); err == nil {
		t.Fatal("停止后 Start 应报错")
	}
}

// TestPanelMount 嵌入形态：挂到宿主 group，前缀路由 + 宿主中间件共存。
func TestPanelMount(t *testing.T) {
	panel, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer panel.Stop(context.Background())
	if err := panel.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	host := gin.New()
	host.Use(func(c *gin.Context) { c.Header("X-Host", "yes"); c.Next() })
	panel.Mount(host.Group("/agentflow"))
	srv := httptest.NewServer(host)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/agentflow/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("embedded health: %v %v", err, resp)
	}
	if resp.Header.Get("X-Host") != "yes" {
		t.Fatal("宿主中间件未生效")
	}
	resp.Body.Close()

	// 注册 Agent 证明 deps 全链路可用
	body := `{"name":"embed-demo","type":"chat","runtime":{"type":"python-http","host":"http://localhost:9"}}`
	r, _ := http.Post(srv.URL+"/agentflow/api/v1/agents", "application/json", strReader(body))
	if r.StatusCode != http.StatusCreated {
		t.Fatalf("注册 Agent: %d", r.StatusCode)
	}
	var created struct {
		ID string `json:"id"`
	}
	json.NewDecoder(r.Body).Decode(&created)
	r.Body.Close()
	if created.ID == "" {
		t.Fatal("未返回 Agent ID")
	}
}

// TestPanelWorkerExecutesTask Start 后 worker 真的在消费：提交任务 -> 落终态。
func TestPanelWorkerExecutesTask(t *testing.T) {
	panel, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := panel.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer panel.Stop(ctx)

	srv := httptest.NewServer(panel.Router())
	defer srv.Close()

	// 假 Agent 恒回 400 -> AGENT_PROTOCOL_VIOLATION（不可重试）-> 直达 failed，
	// 避开 AGENT_UNREACHABLE 的重试退避（默认 3 次重试需 14s+）
	badAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer badAgent.Close()

	agentBody := `{"name":"violator","type":"chat","runtime":{"type":"python-http","host":"` + badAgent.URL + `"}}`
	r, _ := http.Post(srv.URL+"/api/v1/agents", "application/json", strReader(agentBody))
	var agent struct {
		ID string `json:"id"`
	}
	json.NewDecoder(r.Body).Decode(&agent)
	r.Body.Close()

	taskBody := `{"agent_id":"` + agent.ID + `","payload":{"q":"x"}}`
	r, _ = http.Post(srv.URL+"/api/v1/tasks", "application/json", strReader(taskBody))
	var task struct {
		ID string `json:"id"`
	}
	json.NewDecoder(r.Body).Decode(&task)
	r.Body.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(srv.URL + "/api/v1/tasks/" + task.ID)
		if err != nil {
			t.Fatal(err)
		}
		var cur struct {
			Status string `json:"status"`
			Error  struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&cur)
		resp.Body.Close()
		if cur.Status == "failed" {
			if cur.Error.Code != "AGENT_PROTOCOL_VIOLATION" {
				t.Fatalf("错误码 = %q, want AGENT_PROTOCOL_VIOLATION", cur.Error.Code)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("5s 内未到终态，当前 %s", cur.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func strReader(s string) io.Reader { return strings.NewReader(s) }
