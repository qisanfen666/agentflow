// 链路追踪装配。链路模型（诚实边界）：
//
//		客户端 ──► api 根 span ──► [队列解耦，链路天然断开] ──► Execute span ──► agent http span
//		                                                                  └─ traceparent 注入出站请求，
//	                                                                   执行面 Agent 继续上报即全链路
//
// API→worker 跨队列接续需要把 trace context 写进任务记录（公开 API 扩展），
// v0.1 冻结期不做，作为已知缺口记录于此。
package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TraceConfig 追踪装配配置（由 Config.Observability 映射）。
type TraceConfig struct {
	ServiceName  string
	OTLPEndpoint string // 空 = 不启用（全局保持默认 Noop，零开销）
}

// SetupTracing 初始化全局 TracerProvider 与 W3C traceparent 传播器，
// 返回 shutdown 函数（Flush 缓冲 span 后停导出器，进程退出前必须调用）。
//
// 取舍：仅在显式配置 endpoint 时才设置全局——otel 全局默认即 Noop，
// 未启用时不触碰全局状态；嵌入模式下宿主自建 provider 亦不受影响。
func SetupTracing(cfg TraceConfig) (func(context.Context) error, error) {
	// 传播器无论是否启用都设置：出站注入 traceparent 是幂等的头部操作，
	// 未启用 span 时注入空上下文等于无操作。
	otel.SetTextMapPropagator(propagation.TraceContext{})

	if cfg.OTLPEndpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	conn, err := grpc.NewClient(cfg.OTLPEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("otel: otlp grpc 连接失败: %w", err)
	}
	exp, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("otel: otlp 导出器构建失败: %w", err)
	}

	name := cfg.ServiceName
	if name == "" {
		name = "agentflow"
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL, semconv.ServiceName(name))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
