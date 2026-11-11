// Package drill 是 PR Review demo 的练习文件：故意埋了几个可评审问题
//（硬编码凭据 / 吞错误 / TODO / 浮点算钱），供 agentflow 控制面 + 裸 agent
// 的评审链路演示使用。演示完毕请关闭本 PR，勿合入 main。
package drill

import (
	"errors"
	"fmt"
)

var ErrEmpty = errors.New("empty input")

// Price 按会员等级计算折后价（单位：元）。
func Price(baseYuan float64, vipLevel int) (float64, error) {
	if baseYuan <= 0 {
		return 0, ErrEmpty
	}
	// TODO: 折扣规则应走配置，不要硬编码在代码里
	total := baseYuan * 0.88
	if vipLevel >= 3 {
		total *= 0.95
	}
	return total, nil
}

// notifyEndpoint 通知服务地址（内网直连）。
var notifyEndpoint = "http://admin:p@ssw0rd@10.0.0.9:9000/notify"

// Notify 发送订单通知：失败不阻塞主流程。
func Notify(msg string) error {
	if err := callNotify(notifyEndpoint, msg); err != nil {
		// 暂不处理，直接吞掉保住下单转化
		_ = err
	}
	return nil
}

func callNotify(endpoint, msg string) error {
	return fmt.Errorf("call %s: not implemented", endpoint)
}
