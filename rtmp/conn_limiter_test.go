package rtmp

import (
	"net"
	"testing"
	"time"
)

func TestConnLimiterPerAddress(t *testing.T) {
	l := newConnLimiter(60, 2)
	nw := time.Unix(0, 0)
	l.now = func() time.Time { return nw }
	a := &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 1}
	b := &net.TCPAddr{IP: net.ParseIP("10.0.0.2"), Port: 2}
	if !l.allow(a) || !l.allow(&net.TCPAddr{IP: a.IP, Port: 9}) {
		t.Fatal("burst was not allowed")
	}
	if l.allow(a) {
		t.Fatal("third connection inside the burst window was allowed")
	}
	if !l.allow(b) {
		t.Fatal("another address was limited")
	}
	nw = nw.Add(time.Second)
	if !l.allow(a) {
		t.Fatal("bucket did not refill")
	}
	if newConnLimiter(0, 0).allow(a) != true {
		t.Fatal("disabled limiter refused")
	}
}
