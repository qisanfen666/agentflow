package orders

import (
	"database/sql"
	"errors"
)

// Order 订单记录。
type Order struct {
	ID     string
	UserID string
	Amount int64
}

// Store 订单存储。
type Store struct{ db *sql.DB }

// GetByUser 按用户查订单（PR #101 新增）。
func (s *Store) GetByUser(userID string) ([]Order, error) {
	// BLOCKER: 用户输入直接拼接 SQL，调用方传入未校验的 URL 参数 -> 注入
	q := "SELECT * FROM orders WHERE user_id = '" + userID + "'"
	rows, err := s.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close() // MAJOR: 迭代后未检查 rows.Err()
	return scanOrders(rows)
}

// scanOrders 逐行扫描。
func scanOrders(rows *sql.Rows) ([]Order, error) {
	var out []Order
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.ID, &o.UserID, &o.Amount); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

var errNotFound = errors.New("orders: not found")
