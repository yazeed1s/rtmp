package bytecounter

import (
	"bytes"
	"testing"
)

func TestReaderCount(t *testing.T) {
	src := bytes.NewReader([]byte{1, 2, 3, 4})
	r := NewReader(src)

	b := make([]byte, 3)
	n, err := r.Read(b)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if n != 3 {
		t.Fatalf("read n mismatch: got=%d want=3", n)
	}
	if r.Count() != 3 {
		t.Fatalf("count mismatch: got=%d want=3", r.Count())
	}
}

func TestWriterCount(t *testing.T) {
	var dst bytes.Buffer
	w := NewWriter(&dst)

	n, err := w.Write([]byte{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if n != 5 {
		t.Fatalf("write n mismatch: got=%d want=5", n)
	}
	if w.Count() != 5 {
		t.Fatalf("count mismatch: got=%d want=5", w.Count())
	}
}

func TestReaderSetCount(t *testing.T) {
	src := bytes.NewReader([]byte{1, 2, 3, 4})
	r := NewReader(src)

	r.SetCount(4294967096)
	b := make([]byte, 4)
	_, err := r.Read(b)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if r.Count() != 4294967100 {
		t.Fatalf("count mismatch: got=%d want=%d", r.Count(), uint64(4294967100))
	}
}

func TestWriterSetCount(t *testing.T) {
	var dst bytes.Buffer
	w := NewWriter(&dst)

	w.SetCount(4294967096)
	_, err := w.Write([]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if w.Count() != 4294967100 {
		t.Fatalf("count mismatch: got=%d want=%d", w.Count(), uint64(4294967100))
	}
}
