package database

import "github.com/Nystya/distributed-commit/domain"

type Database interface {
	Put(key string, entry *domain.Entry) error
	Get(key string) (*domain.Entry, error)
	GetAllKeys() []string

	Recover() ([]*domain.Entry, error)
	Rollback() error
	Commit() error
}
