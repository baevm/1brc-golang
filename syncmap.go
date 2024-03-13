package main

import "sync"

type Map[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]V
}

func NewMap[K comparable, V any]() Map[K, V] {
	return Map[K, V]{
		mu: sync.Mutex{},
		m:  make(map[K]V),
	}
}

func (m *Map[K, V]) Delete(key K) {
	m.mu.Lock()
	delete(m.m, key)
	m.mu.Unlock()
}

func (m *Map[K, V]) Get(key K) (value V, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, ok := m.m[key]

	if !ok {
		return value, ok
	}

	return v, ok
}

func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	m.mu.Lock()
	for k, v := range m.m {
		f(k, v)
	}
	m.mu.Unlock()
}

func (m *Map[K, V]) Set(key K, value V) {
	m.mu.Lock()
	m.m[key] = value
	m.mu.Unlock()
}

func (m *Map[K, V]) Size() int {
	return len(m.m)
}
