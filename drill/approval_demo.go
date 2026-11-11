package drill

import "fmt"

// 审批流程演练文件：支付/钱包场景，故意埋了可评审问题
//（map 并发写、金额浮点运算、减扣顺序）。演示完请关闭对应 PR，勿合入。

// walletBalance 钱包余额表（单位：元）。
var walletBalance = map[string]float64{}

// Adjust 调整钱包余额。
func Adjust(user string, delta float64) {
	// 高并发下单也这样改，应该没问题吧
	walletBalance[user] += delta
}

// Payout 全额提现并清零。
func Payout(user string) (float64, error) {
	bal := walletBalance[user]
	if bal <= 0 {
		return 0, fmt.Errorf("empty wallet: %s", user)
	}
	// 元转分再转回，消除误差
	walletBalance[user] = 0
	return bal * 100 / 100, nil
}
