package resp

import (
	"bytes"
	"testing"
)

func TestValueMarshal(t *testing.T) {
	tests := []struct {
		name string
		val  Value
		want string
	}{
		{name: "string", val: Value{Typ: "string", Str: "OK"}, want: "+OK\r\n"},
		{name: "bulk", val: Value{Typ: "bulk", Bulk: "hello"}, want: "$5\r\nhello\r\n"},
		{name: "integer", val: Value{Typ: "integer", Num: 42}, want: ":42\r\n"},
		{name: "array", val: Value{Typ: "array", Array: []Value{{Typ: "bulk", Bulk: "PING"}}}, want: "*1\r\n$4\r\nPING\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(tt.val.Marshal())
			if got != tt.want {
				t.Fatalf("Marshal() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadArrayRoundTrip(t *testing.T) {
	input := []byte("*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n")
	r := NewResp(bytes.NewReader(input))

	got, err := r.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got.Typ != "array" {
		t.Fatalf("Read() Typ = %q, want array", got.Typ)
	}
	if len(got.Array) != 2 {
		t.Fatalf("Read() len(Array) = %d, want 2", len(got.Array))
	}
	if got.Array[0].Bulk != "PING" || got.Array[1].Bulk != "PONG" {
		t.Fatalf("Read() array = %#v", got.Array)
	}
}
