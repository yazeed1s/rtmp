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

// initialMessageCap is the first buffer size for a new message. Most RTMP
// messages are smaller; bigger ones double up to their declared length.
const initialMessageCap = 64 << 10

// mbPool holds the small message-buffer headers. It is shared by every
// assembler: a sync.Pool per assembler registers a new pool with the runtime
// each time, and pools (with what they hold) live for two GC cycles, so many
// short-lived assemblers pile up memory.
var mbPool = sync.Pool{New: func() any { return &messageBuffer{} }}

type Assembler struct {
	p   *pool.Pool
	bf  map[uint32]*messageBuffer
	mxs int
	// open is the bytes reserved by unfinished messages on all chunk
	// streams. Each new message reserves its full declared length, so
	// without this cap one connection could open a partial message on
	// thousands of chunk stream ids and reserve gigabytes.
	open    int
	maxOpen int
}

func NewAssembler(p *pool.Pool, mx int) *Assembler {
	a := &Assembler{
		p:       p,
		bf:      make(map[uint32]*messageBuffer),
		mxs:     mx,
		maxOpen: 2 * mx,
	}
	if a.maxOpen <= 0 {
		a.maxOpen = 8 << 20
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
			a.open -= int(b.tl)
			a.rel(b)
			delete(a.bf, h.CSID)
		}

		if a.mxs > 0 && int(h.MessageLength) > a.mxs {
			return nil, ErrMessageTooLarge
		}
		if a.open+int(h.MessageLength) > a.maxOpen {
			return nil, fmt.Errorf("%w: %d bytes already reserved by unfinished messages", ErrMessageTooLarge, a.open)
		}
		a.open += int(h.MessageLength)

		b = a.getMB()
		b.ti = h.MessageTypeID
		b.si = h.MessageStreamID
		b.ts = h.Timestamp
		// start small and grow as bytes arrive. Allocating the declared
		// length up front let a few header bytes force megabytes of
		// allocation.
		b.dt = a.p.Get(min(int(h.MessageLength), initialMessageCap))
		b.tl = h.MessageLength
		a.bf[h.CSID] = b
	}

	if b.tl == 0 {
		a.drop(h.CSID, b)
		return nil, fmt.Errorf("%w: zero message length", ErrBadChunkHeader)
	}

	if b.rc+n > b.tl {
		a.drop(h.CSID, b)
		return nil, fmt.Errorf("%w: chunk exceeds message length: got=%d total=%d", ErrBadChunkHeader, b.rc+n, b.tl)
	}

	// We check growing size too. This is safety even if header total looked valid.
	if a.mxs > 0 && int(b.rc+n) > a.mxs {
		a.drop(h.CSID, b)
		return nil, ErrMessageTooLarge
	}

	// Ownership model:
	// - assembler owns full message buffer from pool
	// - reader writes chunk bytes directly into writable window
	st := int(b.rc)
	ed := st + int(n)
	if cap(b.dt) < ed {
		nc := max(ed, 2*cap(b.dt))
		nc = min(nc, int(b.tl))
		nb := make([]byte, st, nc)
		copy(nb, b.dt[:st])
		a.p.Put(b.dt)
		b.dt = nb
	}
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
	a.open -= int(b.tl)
	b.dt = nil
	a.putMB(b)
	return m, nil
}

// drop forgets an unfinished message and returns its reservation.
func (a *Assembler) drop(csid uint32, b *messageBuffer) {
	a.open -= int(b.tl)
	a.rel(b)
	delete(a.bf, csid)
}

func (a *Assembler) Discard(csid uint32) {
	b := a.bf[csid]
	if b == nil {
		return
	}
	a.drop(csid, b)
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
	v := mbPool.Get()
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
	mbPool.Put(b)
}
