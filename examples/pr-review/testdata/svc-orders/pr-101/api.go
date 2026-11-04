package orders

import (
	"encoding/json"
	"net/http"
)

// HandleGetOrders GET /orders?uid=... （PR #101 新增）。
func HandleGetOrders(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// uid 未做任何校验/归一化即下传存储层
		uid := r.URL.Query().Get("uid")
		data, err := store.GetByUser(uid)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(data)
	}
}
