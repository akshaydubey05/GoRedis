package aof

import (
	"io"
	"os"
	"sync"

	"github.com/akshaydubey05/GoRedis/internal/resp"
)

type AOF struct {
	file *os.File
	mu   sync.Mutex
}

func New(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o666)
	if err != nil {
		return nil, err
	}

	return &AOF{file: f}, nil
}

func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}

func (a *AOF) Write(value resp.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := a.file.Write(value.Marshal()); err != nil {
		return err
	}

	return a.file.Sync()
}

func (a *AOF) Replay(callback func(value resp.Value) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := resp.NewResp(a.file)
	for {
		value, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := callback(value); err != nil {
			return err
		}
	}

	_, err := a.file.Seek(0, io.SeekEnd)
	return err
}