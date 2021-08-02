package database

import (
	"github.com/Nystya/distributed-commit/domain"
	"sync"
)

type MemoryDatabase struct {
	cache map[string]*domain.Entry
	history map[string]*domain.Entry

	lock  	*sync.Mutex
}

func NewMemoryDatabase() *MemoryDatabase {
	return &MemoryDatabase{
		cache: make(map[string]*domain.Entry),
		history: make(map[string]*domain.Entry),
		lock: &sync.Mutex{},
	}
}

func (m *MemoryDatabase) Put(key string, entry *domain.Entry) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.cache[key] = entry

	return nil
}

func (m *MemoryDatabase) Get(key string) (*domain.Entry, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	val, ok := m.cache[key]
	if !ok {
		return nil, &domain.NotFoundError{}
	}

	return val, nil
}

func (m *MemoryDatabase) GetAllKeys() []string {
	keys := make([]string, 0)

	for k := range m.cache {
		keys = append(keys, k)
	}

	return keys
}

func (m *MemoryDatabase) Rollback(key string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.cache[key] = m.history[key]

	return nil
}

func (m *MemoryDatabase) Commit(key string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.history[key] = m.cache[key]

	return nil
}

func (m *MemoryDatabase) Recover() ([]*domain.Entry, error) {
	panic("implement me")
}
