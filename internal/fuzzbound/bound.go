// Package fuzzbound checks that a parser's allocations stay bounded by its
// input. Untrusted length fields are the usual way a small packet turns into
// a gigabyte allocation; this catches that in fuzzing instead of production.
package fuzzbound

import (
	"runtime"
	"testing"
)

// Limit is the most a single parse may allocate beyond 64 bytes per input
// byte. It is far above any honest parse and far below an attack.
const Limit = 16 << 20

// Check runs fn and fails t if it allocated more than the bound.
func Check(t *testing.T, in []byte, fn func()) {
	t.Helper()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	fn()
	runtime.ReadMemStats(&b)
	if got := b.TotalAlloc - a.TotalAlloc; got > uint64(Limit+64*len(in)) {
		t.Fatalf("parse of %d bytes allocated %d bytes", len(in), got)
	}
}
