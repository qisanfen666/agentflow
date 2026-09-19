// 嵌入示例：agentflow 以库形态挂进既有 gin 应用（与 cmd/agentflow-server 的
// 独立部署形态对照）。
//
// 演示点：
//  1. Server.Addr 留空 -> Start 只启动后台组件（worker/delayed 搬运），HTTP 监听归宿主
//  2. Mount 挂载到宿主子路径：/agentflow/health 与 /agentflow/api/v1/*，
//     宿主自身业务路由（/api/*）不受影响
//  3. 生命周期归宿主：信号 -> 宿主 HTTP 先拒新请求 -> Panel.Stop 收后台
//
// 运行: go run ./examples/embed
// 验证: curl http://localhost:9090/api/orders          （宿主业务）
//
//	curl http://localhost:9090/agentflow/health     （挂载的控制面）
package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	agentflow "github.com/qisanfen666/agentflow"
)

func main() {
	// ---------- 宿主：自己的业务服务 ----------
	r := gin.Default()

	orders := r.Group("/api")
	{
		orders.GET("/orders", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"orders": []string{"o-1001", "o-1002"}})
		})
	}

	// ---------- agentflow：装配 + 挂载 ----------
	// Addr 留空是嵌入模式的关键：Panel 不自行监听，只提供路由与后台组件。
	// 换 redis 持久化只需改 Driver 与 Redis 配置。
	panel, err := agentflow.New(agentflow.Config{
		Server:  agentflow.ServerConfig{Mode: gin.ReleaseMode},
		Storage: agentflow.StorageConfig{Driver: "memory"},
	})
	if err != nil {
		log.Fatalf("装配失败: %v", err)
	}
	panel.Mount(r.Group("/agentflow"))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := panel.Start(ctx); err != nil {
		log.Fatalf("启动失败: %v", err)
	}

	// ---------- 宿主托管 HTTP ----------
	srv := &http.Server{Addr: ":9090", Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()
	log.Println("host app listening on :9090 (agentflow mounted at /agentflow)")

	<-ctx.Done()
	log.Println("收到退出信号，开始优雅关停...")

	// 关停顺序与独立部署一致：先拒新 HTTP，再收后台任务组件
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if err := panel.Stop(shutdownCtx); err != nil {
		log.Printf("panel stop: %v", err)
	}
	log.Println("已退出")
}
