// Package store holds GoRedis's in-memory data: strings and hashes,
// safe for many goroutines to use at once.
package store

import (
	"errors"
	"strconv"
	"sync"
)

// strEntry is one string value plus an optional expiry time.
// expireAt is unix milliseconds; 0 means "never expires".
type strEntry struct {
	val      string
	expireAt int64
}

func (e strEntry) expired(now int64) bool {
	return e.expireAt != 0 && now >= e.expireAt
}

// Store holds every key GoRedis knows about. One RWMutex protects
// both maps: RLock for reads (GET, HGET, ...), Lock for writes
// (SET, HSET, DEL, ...). This replaces the two separate mutexes
// (SETsMu, HSETsMu) from the original handler.go.
type Store struct {
	mu     sync.RWMutex
	strs   map[string]strEntry
	hashes map[string]map[string]string
}

func New() *Store {
	return &Store{
		strs:   map[string]strEntry{},
		hashes: map[string]map[string]string{},
	}
}

// ---------- strings ----------

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.strs[key] = strEntry{val: value}
}

// Get returns the value and true, or "" and false if the key is
// missing or has expired. now must be the caller's current time in
// unix milliseconds — the store never calls time.Now() itself, so
// behavior stays deterministic and testable (this matters a lot once
// Raft is in the picture: see Phase 4 of the guide).
func (s *Store) Get(key string, now int64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.strs[key]
	if !ok || e.expired(now) {
		return "", false
	}
	return e.val, true
}

// Del deletes each key (string or hash) and returns how many
// actually existed (and weren't already expired).
func (s *Store) Del(now int64, keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, k := range keys {
		if e, ok := s.strs[k]; ok {
			delete(s.strs, k)
			if !e.expired(now) {
				n++
			}
		}
		if _, ok := s.hashes[k]; ok {
			delete(s.hashes, k)
			n++
		}
	}
	return n
}

// Exists counts how many of the given keys currently exist.
func (s *Store) Exists(now int64, keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, k := range keys {
		if e, ok := s.strs[k]; ok && !e.expired(now) {
			n++
			continue
		}
		if _, ok := s.hashes[k]; ok {
			n++
		}
	}
	return n
}

// Incr adds delta to the integer stored at key (creating it as 0
// first if missing) and returns the new value. Returns an error if
// the existing value isn't a valid integer, matching real Redis's
// "value is not an integer or out of range" behavior.
func (s *Store) Incr(key string, delta, now int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.strs[key]
	var n int64
	if ok && !e.expired(now) {
		parsed, err := strconv.ParseInt(e.val, 10, 64)
		if err != nil {
			return 0, errors.New("value is not an integer or out of range")
		}
		n = parsed
	} else {
		e = strEntry{}
	}

	n += delta
	e.val = strconv.FormatInt(n, 10)
	s.strs[key] = e
	return n, nil
}

// ExpireAt sets key's expiry to an absolute unix-millisecond time.
// Returns true if the key existed and was updated, false if it
// didn't exist.
func (s *Store) ExpireAt(key string, atMs, now int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.strs[key]
	if !ok || e.expired(now) {
		return false
	}
	e.expireAt = atMs
	s.strs[key] = e
	return true
}

// TTL returns seconds remaining before key expires: -2 if the key is
// missing or expired, -1 if it exists but has no expiry, otherwise
// the number of whole seconds left.
func (s *Store) TTL(key string, now int64) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.strs[key]
	if !ok || e.expired(now) {
		return -2
	}
	if e.expireAt == 0 {
		return -1
	}
	remaining := e.expireAt - now
	if remaining < 0 {
		remaining = 0
	}
	return remaining / 1000
}

// DeleteExpired sweeps every string key and removes ones that have
// expired. Call this from a background goroutine every second or so
// (see main.go) so memory is reclaimed even for keys nobody reads.
func (s *Store) DeleteExpired(now int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.strs {
		if e.expired(now) {
			delete(s.strs, k)
		}
	}
}

// ---------- hashes ----------

func (s *Store) HSet(hash, field, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.hashes[hash]; !ok {
		s.hashes[hash] = map[string]string{}
	}
	s.hashes[hash][field] = value
}

func (s *Store) HGet(hash, field string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.hashes[hash][field]
	return value, ok
}

// HGetAll returns a copy of the field->value map for hash. Returning
// a copy (not the live map) means the caller can range over it safely
// without holding our lock the whole time.
func (s *Store) HGetAll(hash string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fields, ok := s.hashes[hash]
	if !ok {
		return nil
	}
	out := make(map[string]string, len(fields))
	for k, v := range fields {
		out[k] = v
	}
	return out
}