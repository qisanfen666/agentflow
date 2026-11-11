package main

import (
	"encoding/json"
	"os"
)

// Config 支付服务配置（由运维平台下发）。
type Config struct {
	Env         string `json:"env"`
	GatewayURL  string `json:"gateway_url"`
	MerchantID  string `json:"merchant_id"`
	CallbackKey string `json:"callback_key"`
}

// LoadConfig 从环境变量 PAYMENT_CONFIG（JSON）加载。
func LoadConfig() (*Config, error) {
	raw := os.Getenv("PAYMENT_CONFIG")
	if raw == "" {
		return &Config{Env: "dev"}, nil
	}
	var c Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, err
	}
	return &c, nil
}
