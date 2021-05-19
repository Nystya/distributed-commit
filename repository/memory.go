package repository

type MemoryDatabase struct {
}

func NewMemoryDatabase() *MemoryDatabase {
	return &MemoryDatabase{}
}

func (m *MemoryDatabase) Put(key string, value interface{}) error {
	return nil
}

func (m *MemoryDatabase) Get(key string) (interface{}, error) {
	return nil, nil
}
