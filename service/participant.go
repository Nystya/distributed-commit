package service

import "github.com/Nystya/distributed-commit/domain"

type Participant interface {
	HandlePrepare(txID string, key string, value []byte) error
	HandleCommit(txID string) error
	HandleAbort(txID string) error

	Get(key string) ([]byte, error)
	GetStatus(txID string) (domain.Status, error)
	Recover() error
}