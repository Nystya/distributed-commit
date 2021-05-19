package repository

type FileDatabaseConfig struct {
}

type FileDatabase struct {
}

func NewFileDatabase(config *FileDatabaseConfig) *FileDatabase {
	return &FileDatabase{}
}

func (f *FileDatabase) Put(key string, value interface{}) error {
	return nil
}

func (f *FileDatabase) Get(key string) (interface{}, error) {
	return nil, nil
}
