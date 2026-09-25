package resp

import (
	"bytes"
	"testing"
)

// TestReadCommand checks that a real RESP command from redis-cli —
// an array of bulk strings — is parsed correctly.
func TestReadCommand(t *testing.T) {
	// *2\r\n$3\r\nGET\r\n$4\r\nname\r\n  ==  GET name
	raw := "*2\r\n$3\r\nGET\r\n$4\r\nname\r\n"
	r := NewResp(bytes.NewBufferString(raw))

	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if v.Typ != "array" {
		t.Fatalf("Typ = %q, want \"array\"", v.Typ)
	}
	if len(v.Array) != 2 {
		t.Fatalf("len(Array) = %d, want 2", len(v.Array))
	}
	if v.Array[0].Bulk != "GET" {
		t.Fatalf("Array[0].Bulk = %q, want \"GET\"", v.Array[0].Bulk)
	}
	if v.Array[1].Bulk != "name" {
		t.Fatalf("Array[1].Bulk = %q, want \"name\"", v.Array[1].Bulk)
	}
}

// TestReadThreeArgCommand checks a longer command, e.g. SET name akshay.
func TestReadThreeArgCommand(t *testing.T) {
	raw := "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\nakshay\r\n"
	r := NewResp(bytes.NewBufferString(raw))

	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	got := []string{v.Array[0].Bulk, v.Array[1].Bulk, v.Array[2].Bulk}
	want := []string{"SET", "name", "akshay"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Array[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestReadEmptyBulk checks a zero-length bulk string, e.g. SET key "".
func TestReadEmptyBulk(t *testing.T) {
	raw := "*1\r\n$0\r\n\r\n"
	r := NewResp(bytes.NewBufferString(raw))

	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if v.Array[0].Bulk != "" {
		t.Fatalf("Bulk = %q, want empty string", v.Array[0].Bulk)
	}
}

// ---------- marshal tests: reply -> bytes ----------

func TestMarshalString(t *testing.T) {
	v := Value{Typ: "string", Str: "OK"}
	got := string(v.Marshal())
	want := "+OK\r\n"
	if got != want {
		t.Fatalf("Marshal = %q, want %q", got, want)
	}
}

func TestMarshalBulk(t *testing.T) {
	v := Value{Typ: "bulk", Bulk: "akshay"}
	got := string(v.Marshal())
	want := "$6\r\nakshay\r\n"
	if got != want {
		t.Fatalf("Marshal = %q, want %q", got, want)
	}
}

func TestMarshalNull(t *testing.T) {
	v := Value{Typ: "null"}
	got := string(v.Marshal())
	want := "$-1\r\n"
	if got != want {
		t.Fatalf("Marshal = %q, want %q", got, want)
	}
}

func TestMarshalError(t *testing.T) {
	v := Value{Typ: "error", Str: "ERR something broke"}
	got := string(v.Marshal())
	want := "-ERR something broke\r\n"
	if got != want {
		t.Fatalf("Marshal = %q, want %q", got, want)
	}
}

func TestMarshalInteger(t *testing.T) {
	v := Value{Typ: "integer", Num: 42}
	got := string(v.Marshal())
	want := ":42\r\n"
	if got != want {
		t.Fatalf("Marshal = %q, want %q", got, want)
	}
}

func TestMarshalArray(t *testing.T) {
	v := Value{Typ: "array", Array: []Value{
		{Typ: "bulk", Bulk: "first_name"},
		{Typ: "bulk", Bulk: "Akshay"},
	}}
	got := string(v.Marshal())
	want := "*2\r\n$10\r\nfirst_name\r\n$6\r\nAkshay\r\n"
	if got != want {
		t.Fatalf("Marshal = %q, want %q", got, want)
	}
}

func TestMarshalEmptyArray(t *testing.T) {
	v := Value{Typ: "array", Array: []Value{}}
	got := string(v.Marshal())
	want := "*0\r\n"
	if got != want {
		t.Fatalf("Marshal = %q, want %q", got, want)
	}
}

// TestRoundTrip writes a Value out and reads it back, checking both
// halves of the protocol agree with each other. This is the kind of
// test that catches "writer and reader silently drifted apart" bugs.
func TestRoundTrip(t *testing.T) {
	original := Value{Typ: "array", Array: []Value{
		{Typ: "bulk", Bulk: "HSET"},
		{Typ: "bulk", Bulk: "user:1"},
		{Typ: "bulk", Bulk: "first_name"},
		{Typ: "bulk", Bulk: "Akshay"},
	}}

	var buf bytes.Buffer
	buf.Write(original.Marshal())

	r := NewResp(&buf)
	got, err := r.Read()
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}

	if len(got.Array) != len(original.Array) {
		t.Fatalf("len(Array) = %d, want %d", len(got.Array), len(original.Array))
	}
	for i := range original.Array {
		if got.Array[i].Bulk != original.Array[i].Bulk {
			t.Fatalf("Array[%d].Bulk = %q, want %q", i, got.Array[i].Bulk, original.Array[i].Bulk)
		}
	}
}