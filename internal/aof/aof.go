// Package aof implements append-only-file persistence: every
// mutating command is written to disk, and on startup the file is
// replayed to rebuild the in-memory store.
package aof

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"

	"github.com/akshaydubey05/GoRedis/internal/resp"
)

type AOF struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func New(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	a := &AOF{
		file: f,
		rd:   bufio.NewReader(f),
	}

	// Background goroutine: fsync to disk once a second, same as your
	// original. This trades a little durability (up to ~1s of writes
	// could be lost on a hard crash) for much better write throughput
	// than syncing on every single command.
	go func() {
		for {
			a.mu.Lock()
			a.file.Sync()
			a.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	return a, nil
}

func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}

// Write appends one command (as its original RESP wire form) to the
// file. The server package calls this before executing any mutating
// command.
func (a *AOF) Write(v resp.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err := a.file.Write(v.Marshal())
	return err
}

// Read replays every command previously written to the file, calling
// callback for each one. Call this once at startup, before Listen.
func (a *AOF) Read(callback func(v resp.Value)) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	reader := resp.NewResp(a.file)
	for {
		v, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		callback(v)
	}
	return nil
}