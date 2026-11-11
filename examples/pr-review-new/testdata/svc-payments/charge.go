package main

import (
	"context"
	"time"
)

// Charge 一次扣款请求。
type Charge struct {
	OrderID string
	Amount  int64 // 分
	UserID  string
}

// Gateway 收单网关抽象。
type Gateway interface {
	Pay(ctx context.Context, ch Charge) (txnID string, err error)
}

// Service 支付服务。
type Service struct {
	gw Gateway
}

func (s *Service) Charge(ctx context.Context, ch Charge) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.gw.Pay(cctx, ch)
}
