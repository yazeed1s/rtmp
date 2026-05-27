package message

import (
	"errors"
	"fmt"
	"sync"

	"github.com/yazeed1s/rtmp/internal/chunk"
	"github.com/yazeed1s/rtmp/internal/pool"
)

var (
	ErrBadChunkHeader  = errors.New("message: bad chunk header")
	ErrMessageTooLarge = errors.New("message: message too large")
)

type RawMessage struct {
	TypeID    uint8
	StreamID  uint32
	Timestamp uint32
	Payload   []byte
	p         *pool.Pool
}

func (m *RawMessage) Release() {
	if m == nil || m.p == nil || m.Payload == nil {
		return
	}
	m.p.Put(m.Payload)
	m.Payload = nil
}

type messageBuffer struct {
	ti uint8
	si uint32
	ts uint32
	dt []byte
	rc uint32
	tl uint32
}

type Assembler struct {
	p   *pool.Pool
	bf  map[uint32]*messageBuffer
	mxs int
	mbp sync.Pool
}

func NewAssembler(p *pool.Pool, mx int) *Assembler {
	a := &Assembler{
		p:   p,
		bf:  make(map[uint32]*messageBuffer),
		mxs: mx,
	}
	a.mbp.New = func() any {
		return &messageBuffer{}
	}
	return a
}

func (a *Assembler) Feed(h chunk.ChunkHeader, p []byte) (*RawMessage, error) {
	return a.feedRead(h, uint32(len(p)), func(dst []byte) error {
		copy(dst, p)
		return nil
	})
}

func (a *Assembler) FeedRead(h chunk.ChunkHeader, n uint32, rd func([]byte) error) (*RawMessage, error) {
	return a.feedRead(h, n, rd)
}

// feedRead builds the final message.
// Performance trick is that we do not create new memory (`make([]byte)`) for every message.
// We take a buffer from the pool, and the chunk reader writes directly into this buffer, zero copy.
// When user finishes with the packet, they must call Release() to put it back in the pool.
func (a *Assembler) feedRead(h chunk.ChunkHeader, n uint32, rd func([]byte) error) (*RawMessage, error) {
	b := a.bf[h.CSID]

	// New message can start before old one is complete, so we drop old partial bytes and start fresh.
	if h.IsNewMessage || b == nil {
		if b != nil {
			a.rel(b)
		}

		if a.mxs > 0 && int(h.MessageLength) > a.mxs {
			return nil, ErrMessageTooLarge
		}

		b = a.getMB()
		b.ti = h.MessageTypeID
		b.si = h.MessageStreamID
		b.ts = h.Timestamp
		b.dt = a.p.Get(int(h.MessageLength))
		b.tl = h.MessageLength
		a.bf[h.CSID] = b
	}

	if b.tl == 0 {
		a.rel(b)
		delete(a.bf, h.CSID)
		return nil, fmt.Errorf("%w: zero message length", ErrBadChunkHeader)
	}

	if b.rc+n > b.tl {
		a.rel(b)
		delete(a.bf, h.CSID)
		return nil, fmt.Errorf("%w: chunk exceeds message length: got=%d total=%d", ErrBadChunkHeader, b.rc+n, b.tl)
	}

	// We check growing size too. This is safety even if header total looked valid.
	if a.mxs > 0 && int(b.rc+n) > a.mxs {
		a.rel(b)
		delete(a.bf, h.CSID)
		return nil, ErrMessageTooLarge
	}

	// Ownership model:
	// - assembler owns full message buffer from pool
	// - reader writes chunk bytes directly into writable window
	st := int(b.rc)
	ed := st + int(n)
	b.dt = b.dt[:ed]
	if err := rd(b.dt[st:ed]); err != nil {
		// Keep buffer state valid when read fails.
		b.dt = b.dt[:st]
		return nil, err
	}
	b.rc += n

	if b.rc != b.tl {
		return nil, nil
	}

	m := &RawMessage{
		TypeID:    b.ti,
		StreamID:  b.si,
		Timestamp: b.ts,
		Payload:   b.dt,
		p:         a.p,
	}
	delete(a.bf, h.CSID)
	b.dt = nil
	a.putMB(b)
	return m, nil
}

func (a *Assembler) Discard(csid uint32) {
	b := a.bf[csid]
	if b == nil {
		return
	}
	a.rel(b)
	delete(a.bf, csid)
}

func (a *Assembler) rel(b *messageBuffer) {
	if b == nil || b.dt == nil {
		return
	}
	a.p.Put(b.dt)
	b.dt = nil
	a.putMB(b)
}

func (a *Assembler) getMB() *messageBuffer {
	v := a.mbp.Get()
	if v == nil {
		return &messageBuffer{}
	}
	b, ok := v.(*messageBuffer)
	if !ok || b == nil {
		return &messageBuffer{}
	}
	return b
}

func (a *Assembler) putMB(b *messageBuffer) {
	if b == nil {
		return
	}
	*b = messageBuffer{}
	a.mbp.Put(b)
}
