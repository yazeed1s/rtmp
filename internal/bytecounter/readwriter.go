package bytecounter

import "io"

// ReadWriter wraps io.ReadWriter and counts both read and written bytes.
type ReadWriter struct {
	*Reader
	*Writer
}

func NewReadWriter(rw io.ReadWriter) *ReadWriter {
	return &ReadWriter{
		Reader: NewReader(rw),
		Writer: NewWriter(rw),
	}
}
