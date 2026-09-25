package store

import (
	"sync"
	"testing"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("name", "akshay")
	v, ok := s.Get("name", 0)
	if !ok || v != "akshay" {
		t.Fatalf("got (%q, %v), want (\"akshay\", true)", v, ok)
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	if _, ok := s.Get("nope", 0); ok {
		t.Fatal("expected missing key to return ok=false")
	}
}

func TestDelAndExists(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")
	if n := s.Exists(0, "a", "b", "c"); n != 2 {
		t.Fatalf("Exists = %d, want 2", n)
	}
	if n := s.Del(0, "a", "c"); n != 1 {
		t.Fatalf("Del = %d, want 1", n)
	}
	if n := s.Exists(0, "a", "b"); n != 1 {
		t.Fatalf("Exists after Del = %d, want 1", n)
	}
}

func TestIncr(t *testing.T) {
	s := New()
	n, err := s.Incr("counter", 1, 0)
	if err != nil || n != 1 {
		t.Fatalf("Incr = (%d, %v), want (1, nil)", n, err)
	}
	n, err = s.Incr("counter", 5, 0)
	if err != nil || n != 6 {
		t.Fatalf("Incr = (%d, %v), want (6, nil)", n, err)
	}
}

func TestIncrOnNonInteger(t *testing.T) {
	s := New()
	s.Set("name", "akshay")
	if _, err := s.Incr("name", 1, 0); err == nil {
		t.Fatal("expected an error incrementing a non-integer value")
	}
}

func TestTTLAndExpiry(t *testing.T) {
	s := New()
	s.Set("a", "1")
	if !s.ExpireAt("a", 5000, 0) {
		t.Fatal("ExpireAt should return true for an existing key")
	}
	if ttl := s.TTL("a", 3000); ttl != 2 {
		t.Fatalf("TTL = %d, want 2", ttl)
	}
	if _, ok := s.Get("a", 4999); !ok {
		t.Fatal("key should still exist just before expiry")
	}
	if _, ok := s.Get("a", 5000); ok {
		t.Fatal("key should be gone exactly at its expiry time")
	}
	if ttl := s.TTL("a", 5000); ttl != -2 {
		t.Fatalf("TTL of expired key = %d, want -2", ttl)
	}
}

func TestTTLNoExpiry(t *testing.T) {
	s := New()
	s.Set("a", "1")
	if ttl := s.TTL("a", 0); ttl != -1 {
		t.Fatalf("TTL of key with no expiry = %d, want -1", ttl)
	}
}

func TestHashes(t *testing.T) {
	s := New()
	s.HSet("user:1", "first_name", "Akshay")
	s.HSet("user:1", "last_name", "Dubey")

	v, ok := s.HGet("user:1", "first_name")
	if !ok || v != "Akshay" {
		t.Fatalf("HGet = (%q, %v), want (\"Akshay\", true)", v, ok)
	}

	all := s.HGetAll("user:1")
	if len(all) != 2 || all["first_name"] != "Akshay" || all["last_name"] != "Dubey" {
		t.Fatalf("HGetAll = %v, unexpected", all)
	}
}

// TestConcurrentIncr is the important one: it proves the mutex
// actually prevents lost updates when many goroutines write at once.
// Run this with -race to also catch any data races.
func TestConcurrentIncr(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	const goroutines = 100
	const perGoroutine = 1000

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				s.Incr("counter", 1, 0)
			}
		}()
	}
	wg.Wait()

	v, _ := s.Get("counter", 0)
	want := strconv_Itoa(goroutines * perGoroutine)
	if v != want {
		t.Fatalf("counter = %q, want %q", v, want)
	}
}

func strconv_Itoa(n int) string {
	// tiny local helper so this test file doesn't need an extra import
	// just for one call; feel free to replace with strconv.Itoa if you
	// prefer importing strconv here directly.
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}