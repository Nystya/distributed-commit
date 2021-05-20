package database

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Nystya/distributed-commit/domain"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

type WriteAheadLogConfig struct {
	Dir 		string
	MaxFileSize int64
	Prefix		string
}

type WriteAheadLog struct {
	dir 		string
	fileList	[]string
	activeFile	string
	nextIndex	int
	maxFileSize int64
	prefix		string
}

const KiloByte = 1024

func NewFileDatabase(config *WriteAheadLogConfig) (*WriteAheadLog, error) {
	fileList, err := os.ReadDir(config.Dir)
	if err != nil {
		return nil, err
	}

	fileListNames := make([]string, 0)
	for _, file := range fileList {
		fileListNames = append(fileListNames, config.Dir + "/" + file.Name())
	}

	sort.Slice(fileListNames, func(i, j int) bool {
		idx1, _ := strconv.ParseInt(strings.Split(fileListNames[i], "_")[0], 10, 32)
		idx2, _ := strconv.ParseInt(strings.Split(fileListNames[j], "_")[0], 10, 32)

		if idx1 < idx2 {
			return true
		}

		return false
	})

	var activeFile 	string
	var nextIndex	int
	if len(fileListNames) > 0 {
		activeFile = fileListNames[len(fileListNames) - 1]
		nextIndex = len(fileListNames)
	} else {
		activeFile = ""
		nextIndex = 0
	}

	return &WriteAheadLog{
		dir: 			config.Dir,
		fileList: 		fileListNames,
		activeFile: 	activeFile,
		nextIndex: 		nextIndex,
		maxFileSize: 	config.MaxFileSize * KiloByte,
		prefix:			config.Prefix,
	}, nil
}

func (f *WriteAheadLog) Recover() ([]*domain.Entry, error) {
	entryList := make([]*domain.Entry, 0)

	for _, file := range f.fileList {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}

		reader := bufio.NewReader(f)

		for {
			line, err := reader.ReadBytes('\n')
			if err == io.EOF {
				break
			}

			if err != nil {
				return nil, err
			}

			entry := &domain.Entry{}
			err = json.Unmarshal(line, entry)
			if err != nil {
				return nil, err
			}

			entryList = append(entryList, entry)
		}
	}

	return entryList, nil
}

func (f *WriteAheadLog) Put(key string, entry *domain.Entry) error {
	if f.nextIndex == 0 {
		if err := f.CreateNextFile(); err != nil {
			return err
		}
	}

	stat, err := os.Stat(fmt.Sprintf("%v", f.activeFile))
	if err != nil {
		return err
	}

	// Check if we should go to next wal file. Default value is 100KB
	if stat.Size() >= f.maxFileSize {
		if err = f.CreateNextFile(); err != nil {
			return err
		}
	}

	marshal, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(fmt.Sprintf("%v", f.activeFile), os.O_RDWR | os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	defer file.Close()

	jsonData := string(marshal) + "\n"

	curLen := 0

	for curLen < len(jsonData) {
		writtenLen, err := file.WriteString(jsonData[curLen:])
		if err != nil {
			return err
		}

		curLen += writtenLen
	}

	return nil
}

func (f *WriteAheadLog) CreateNextFile() error {
	create, err := os.Create(fmt.Sprintf("%v/%v_%v_wal", f.dir, f.nextIndex, f.prefix))
	if err != nil {
		return err
	}
	f.activeFile = create.Name()
	f.fileList = append(f.fileList, f.activeFile)
	f.nextIndex++

	return create.Close()
}

func (f *WriteAheadLog) Get(key string) (*domain.Entry, error) {
	panic("implement me")
}

func (f *WriteAheadLog) Rollback() error {
	panic("implement me")
}

func (f *WriteAheadLog) Commit() error {
	panic("implement me")
}

func (f *WriteAheadLog) GetAllKeys() []string {
	panic("implement me")
}


