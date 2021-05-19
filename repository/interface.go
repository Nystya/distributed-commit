package repository

type Database interface {
	Put(key string, value interface{}) error
	Get(key string) (interface{}, error)
}
