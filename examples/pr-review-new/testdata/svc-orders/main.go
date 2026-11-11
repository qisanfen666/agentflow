package main

import (
	"log"
	"net/http"
)

func main() {
	store := NewStore()
	http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleCreate(w, r, store)
		case http.MethodGet:
			handleList(w, r, store)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	log.Println("svc-orders listening :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
