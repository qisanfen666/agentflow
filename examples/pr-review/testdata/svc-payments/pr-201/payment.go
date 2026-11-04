package payments

import "errors"

// ErrInsufficient 余额不足。
var ErrInsufficient = errors.New("payments: insufficient balance")

// Account 支付账户（demo 简化：余额为账户内字段）。
type Account struct {
	ID      string
	Balance float64
}

// Charge 扣款（PR #201 新增）。
//
// BLOCKER: float64 计算金额存在精度丢失；且读-改-写非原子，
// 并发扣款会丢失更新（余额扣少/超扣）。
func Charge(a *Account, amount float64) error {
	if a.Balance < amount {
		return ErrInsufficient
	}
	a.Balance = a.Balance - amount
	return nil
}
