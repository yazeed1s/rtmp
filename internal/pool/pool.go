package pool

import "sync"

type Pool struct {
	p sync.Pool
}

func New() *Pool {
	return &Pool{
		p: sync.Pool{
			New: func() any {
				return make([]byte, 0, 65536)
			},
		},
	}
}

func (p *Pool) Get(minCap int) []byte {
	b := p.p.Get().([]byte)
	if cap(b) >= minCap {
		return b[:0]
	}
	return make([]byte, 0, minCap)
}

func (p *Pool) Put(b []byte) {
	if cap(b) > 131072 {
		return
	}
	p.p.Put(b[:0])
}
