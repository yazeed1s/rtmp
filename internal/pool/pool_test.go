package pool

import "testing"

func TestGetMinCap(t *testing.T) {
	p := New()

	b := p.Get(1024)
	if len(b) != 0 {
		t.Fatalf("len mismatch: got=%d want=0", len(b))
	}
	if cap(b) < 1024 {
		t.Fatalf("cap too small: got=%d want>=%d", cap(b), 1024)
	}
}

func TestGetPutNoAlloc(t *testing.T) {
	p := New()

	// warm up
	b := p.Get(1024)
	p.Put(b)

	n := testing.AllocsPerRun(100, func() {
		b := p.Get(1024)
		p.Put(b)
	})
	if n > 1 {
		t.Fatalf("allocs too high: got=%f want<=1", n)
	}
}

func TestPutBigNoPanic(t *testing.T) {
	p := New()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic: %v", r)
		}
	}()

	b := make([]byte, 0, 131073)
	p.Put(b)
}
