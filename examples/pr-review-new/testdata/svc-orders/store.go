package main

import (
	"errors"
	"sync"
)

// Store 内存订单存储（demo 用途，无持久化）。
type Store struct {
	mu     sync.RWMutex
	orders map[string]Order
}

func NewStore() *Store {
	return &Store{orders: map[string]Order{}}
}

var errConflict = errors.New("order already exists")

func (s *Store) Save(o Order) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.orders[o.ID]; ok {
		return errConflict
	}
	s.orders[o.ID] = o
	return nil
}

func (s *Store) List() ([]Order, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Order, 0, len(s.orders))
	for _, o := range s.orders {
		out = append(out, o)
	}
	return out, nil
}
