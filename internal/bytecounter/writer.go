package bytecounter

import (
	"io"
	"sync/atomic"
)

// Writer wraps io.Writer and counts total written bytes.
type Writer struct {
	w io.Writer
	c atomic.Uint64
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.c.Add(uint64(n))
	return n, err
}

func (w *Writer) Count() uint64 {
	return w.c.Load()
}

func (w *Writer) SetCount(v uint64) {
	w.c.Store(v)
}
