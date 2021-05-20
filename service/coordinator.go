package service

import "github.com/Nystya/distributed-commit/domain"

type Coordinator interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Gather(key string) (map[string][]byte, error)

	GetStatus(txID string) (domain.Status, error)
}
