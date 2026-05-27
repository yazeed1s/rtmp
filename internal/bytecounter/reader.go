package bytecounter

import (
	"io"
	"sync/atomic"
)

// Reader wraps io.Reader and counts total read bytes.
// We use atomic.Uint64 here, which allows the main thread to update the count fast,
// and other threads can read it without locking a slow mutex.
type Reader struct {
	r io.Reader
	c atomic.Uint64
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.c.Add(uint64(n))
	return n, err
}

func (r *Reader) Count() uint64 {
	return r.c.Load()
}

func (r *Reader) SetCount(v uint64) {
	r.c.Store(v)
}
