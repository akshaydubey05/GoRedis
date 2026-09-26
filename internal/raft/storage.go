package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"os"
	"sync"
)

// Storage is how a Node persists the state it must never forget
// across a restart: currentTerm, votedFor, and the log. Losing any
// of these can violate Raft's safety guarantees (see the guide's
// "why persist votedFor" note), so writes here should be treated as
// seriously as a database's own durability.
type Storage interface {
	Save(data []byte) error
	Load() ([]byte, error) // returns nil, nil if nothing has been saved yet
}

// ---------- in-memory storage, for tests ----------

type MemStorage struct {
	mu   sync.Mutex
	data []byte
}

func (m *MemStorage) Save(d []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append([]byte(nil), d...) // copy, so callers can't mutate our stored bytes
	return nil
}

func (m *MemStorage) Load() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.data...), nil
}

// ---------- file storage, for real nodes ----------

// FileStorage saves to disk using the write-temp-file-then-rename
// trick: if the process crashes mid-write, the original file is
// untouched, so we never end up with a half-written, corrupt state
// file. os.Rename on the same filesystem is atomic.
type FileStorage struct {
	Path string
}

func (f FileStorage) Save(d []byte) error {
	tmp := f.Path + ".tmp"

	fh, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := fh.Write(d); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil { // force to physical disk before renaming
		fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, f.Path)
}

func (f FileStorage) Load() ([]byte, error) {
	data, err := os.ReadFile(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil // first ever startup: nothing saved yet
	}
	return data, err
}

// ---------- what actually gets encoded ----------

// persistedState is the shape we serialize with gob (Go's built-in
// binary encoding — no external dependency needed).
type persistedState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

func encodeState(term, votedFor int, log []LogEntry) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistedState{term, votedFor, log}); err != nil {
		// Encoding to an in-memory buffer basically can't fail for
		// these simple types; if it does, something is very wrong.
		panic("raft: failed to encode persisted state: " + err.Error())
	}
	return buf.Bytes()
}

func decodeState(data []byte) (term, votedFor int, log []LogEntry, err error) {
	var p persistedState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&p); err != nil {
		return 0, 0, nil, err
	}
	return p.CurrentTerm, p.VotedFor, p.Log, nil
}