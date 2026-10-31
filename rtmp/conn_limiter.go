package rtmp

import (
	"net"
	"sync"
	"time"
)

// connLimiter is a token bucket per remote IP. It stops one address from
// opening connections faster than a publisher ever needs to, before any
// handshake work or session memory is spent on it.
type connLimiter struct {
	mu    sync.Mutex
	rate  float64 // tokens per second
	burst float64
	mp    map[string]*bucket
	seen  int
	now   func() time.Time
}

type bucket struct {
	tok  float64
	last time.Time
}

// limiterIdle is how long an address keeps its bucket after its last
// connection. Pruning keeps the map bounded by recent addresses only.
const limiterIdle = 10 * time.Minute

func newConnLimiter(perMinute, burst int) *connLimiter {
	if perMinute <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = perMinute
	}
	return &connLimiter{
		rate: float64(perMinute) / 60, burst: float64(burst),
		mp: make(map[string]*bucket), now: time.Now,
	}
}

func (l *connLimiter) allow(addr net.Addr) bool {
	if l == nil {
		return true
	}
	ip := addr.String()
	if ta, ok := addr.(*net.TCPAddr); ok {
		ip = ta.IP.String()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	nw := l.now()
	l.seen++
	if l.seen%1024 == 0 {
		for k, b := range l.mp {
			if nw.Sub(b.last) > limiterIdle {
				delete(l.mp, k)
			}
		}
	}
	b := l.mp[ip]
	if b == nil {
		b = &bucket{tok: l.burst, last: nw}
		l.mp[ip] = b
	}
	b.tok += nw.Sub(b.last).Seconds() * l.rate
	if b.tok > l.burst {
		b.tok = l.burst
	}
	b.last = nw
	if b.tok < 1 {
		return false
	}
	b.tok--
	return true
}
