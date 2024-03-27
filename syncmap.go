package main

import (
	"sync"

	"github.com/dolthub/swiss"
)

type Map[K comparable, V any] struct {
	mu sync.Mutex
	m  *swiss.Map[K, V]
}

func NewMap[K comparable, V any]() Map[K, V] {
	return Map[K, V]{
		mu: sync.Mutex{},
		m:  swiss.NewMap[K, V](413),
	}
}

func (m *Map[K, V]) Get(key K) (value V, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, ok := m.m.Get(key)

	if !ok {
		return value, ok
	}

	return v, ok
}

func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	m.mu.Lock()
	// for k, v := range m.m {
	// 	f(k, v)
	// }
	m.m.Iter(f)
	m.mu.Unlock()
}

func (m *Map[K, V]) Set(key K, value V) {
	m.mu.Lock()
	m.m.Put(key, value)
	m.mu.Unlock()
}

func (m *Map[K, V]) Size() int {
	return m.m.Count()
}
