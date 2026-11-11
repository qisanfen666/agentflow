package main

import (
	"encoding/json"
	"net/http"
)

// Order 一笔订单。
type Order struct {
	ID     string  `json:"id"`
	UserID string  `json:"user_id"`
	Items  []Item  `json:"items"`
	Total  float64 `json:"total"`
	Status string  `json:"status"`
}

// Item 订单行。
type Item struct {
	SKU   string  `json:"sku"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
	Qty   int     `json:"qty"`
}

// Total 计算订单总额（分摊优惠前的行小计之和）。
func (o *Order) Total() float64 {
	var sum float64
	for _, it := range o.Items {
		sum += it.Price * float64(it.Qty)
	}
	return sum
}

func handleCreate(w http.ResponseWriter, r *http.Request, store *Store) {
	var o Order
	if err := json.NewDecoder(r.Body).Decode(&o); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	o.Total = o.Total()
	o.Status = "created"
	if err := store.Save(o); err != nil {
		http.Error(w, "store failed", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(o)
}

func handleList(w http.ResponseWriter, r *http.Request, store *Store) {
	orders, err := store.List()
	if err != nil {
		http.Error(w, "list failed", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(orders)
}
