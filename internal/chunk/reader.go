package chunk

import (
	"bufio"
	"fmt"
	"io"
)

type chunkStreamState struct {
	// ts is absolute RTMP timestamp for current chunk stream.
	ts uint32
	// dt is last timestamp delta. We reuse it when a new message starts with fmt3.
	dt uint32
	// ml is full RTMP message length (not this chunk payload size).
	ml uint32
	mt uint8
	ms uint32
	// ex means previous fmt0/1/2 used extended timestamp field.
	ex bool
	// rd tracks how many bytes of current RTMP message we already read.
	rd uint32
	ok bool
}

type Reader struct {
	r  *bufio.Reader
	st map[uint32]*chunkStreamState
	cs uint32

	ps *chunkStreamState
	pn uint32
}

type ChunkHeader struct {
	CSID            uint32
	MessageTypeID   uint8
	MessageStreamID uint32
	Timestamp       uint32
	MessageLength   uint32
	IsNewMessage    bool
}

func NewReader(r *bufio.Reader) *Reader {
	return &Reader{
		r:  r,
		st: make(map[uint32]*chunkStreamState),
		cs: 128,
	}
}

func (r *Reader) SetChunkSize(sz uint32) {
	if sz == 0 {
		return
	}
	r.cs = sz
}

func (r *Reader) ReadChunkHeader() (ChunkHeader, uint32, error) {
	var h ChunkHeader

	if r.ps != nil {
		return h, 0, fmt.Errorf("chunk: read body before next header")
	}

	b0, err := r.readU8()
	if err != nil {
		return h, 0, err
	}

	fm := b0 >> 6
	csid, err := r.readCSID(b0 & 0x3F)
	if err != nil {
		return h, 0, err
	}
	h.CSID = csid

	s := r.st[csid]
	if s == nil {
		s = &chunkStreamState{}
		r.st[csid] = s
	}

	switch fm {
	case 0:
		// fmt0 starts a new message and sets full header fields.
		if err := r.readFmt0(s); err != nil {
			return h, 0, err
		}
		s.rd = 0
		h.IsNewMessage = true

	case 1:
		// fmt1 starts a new message and inherits stream id from previous state.
		if !s.ok {
			return h, 0, fmt.Errorf("chunk: fmt1 without state for csid=%d", csid)
		}
		if err := r.readFmt1(s); err != nil {
			return h, 0, err
		}
		s.rd = 0
		h.IsNewMessage = true

	case 2:
		// fmt2 starts a new message and inherits stream id, type, and length.
		if !s.ok {
			return h, 0, fmt.Errorf("chunk: fmt2 without state for csid=%d", csid)
		}
		if err := r.readFmt2(s); err != nil {
			return h, 0, err
		}
		s.rd = 0
		h.IsNewMessage = true

	case 3:
		if !s.ok {
			return h, 0, fmt.Errorf("chunk: fmt3 without state for csid=%d", csid)
		}

		// fmt3 can be a continuation chunk or first chunk of next message.
		if s.rd == s.ml {
			s.rd = 0
			// Same as ref behavior: when new message starts with fmt3,
			// timestamp moves by last known delta.
			s.ts += s.dt
			h.IsNewMessage = true
		}

		if s.ex {
			// We must consume this field when previous fmt0/1/2 had extended ts.
			// Most senders repeat this field for all fmt3 chunks.
			if _, err := r.readU32BE(); err != nil {
				return h, 0, err
			}
		}

	default:
		return h, 0, fmt.Errorf("chunk: bad fmt=%d", fm)
	}

	h.MessageTypeID = s.mt
	h.MessageStreamID = s.ms
	h.Timestamp = s.ts
	h.MessageLength = s.ml

	if s.ml == 0 {
		return h, 0, fmt.Errorf("chunk: zero message length on csid=%d", csid)
	}
	if s.rd > s.ml {
		return h, 0, fmt.Errorf("chunk: bad state rd=%d ml=%d csid=%d", s.rd, s.ml, csid)
	}

	rm := s.ml - s.rd
	n := minU32(r.cs, rm)

	r.ps = s
	r.pn = n
	return h, n, nil
}

func (r *Reader) ReadChunkBody(p []byte) error {
	if r.ps == nil {
		return fmt.Errorf("chunk: read header before body")
	}
	if uint32(len(p)) != r.pn {
		return fmt.Errorf("chunk: body len mismatch: got=%d want=%d", len(p), r.pn)
	}

	if _, err := io.ReadFull(r.r, p); err != nil {
		r.ps = nil
		r.pn = 0
		return err
	}

	r.ps.rd += r.pn
	r.ps = nil
	r.pn = 0
	return nil
}

func (r *Reader) ReadChunk() (ChunkHeader, []byte, error) {
	h, n, err := r.ReadChunkHeader()
	if err != nil {
		return h, nil, err
	}

	p := make([]byte, n)
	if err := r.ReadChunkBody(p); err != nil {
		return h, nil, err
	}
	return h, p, nil
}

func (r *Reader) readCSID(raw uint8) (uint32, error) {
	switch raw {
	case 0:
		// 2-byte basic header: csid = 64 + next byte.
		b, err := r.readU8()
		if err != nil {
			return 0, err
		}
		return uint32(b) + 64, nil
	case 1:
		// 3-byte basic header: csid = 64 + little-endian uint16(next 2 bytes).
		b2, err := r.readU8()
		if err != nil {
			return 0, err
		}
		b3, err := r.readU8()
		if err != nil {
			return 0, err
		}
		return uint32(b3)*256 + uint32(b2) + 64, nil
	default:
		return uint32(raw), nil
	}
}

func (r *Reader) readFmt0(s *chunkStreamState) error {
	ts24, err := r.readU24BE()
	if err != nil {
		return err
	}
	ml, err := r.readU24BE()
	if err != nil {
		return err
	}
	mt, err := r.readU8()
	if err != nil {
		return err
	}
	// Spec detail: message stream id is little-endian in fmt0.
	ms, err := r.readU32LE()
	if err != nil {
		return err
	}

	s.ts = ts24
	s.ex = false
	if ts24 == 0x00FFFFFF {
		ext, err := r.readU32BE()
		if err != nil {
			return err
		}
		s.ts = ext
		s.ex = true
	}

	s.dt = 0
	s.ml = ml
	s.mt = mt
	s.ms = ms
	s.ok = true
	return nil
}

func (r *Reader) readFmt1(s *chunkStreamState) error {
	d24, err := r.readU24BE()
	if err != nil {
		return err
	}
	ml, err := r.readU24BE()
	if err != nil {
		return err
	}
	mt, err := r.readU8()
	if err != nil {
		return err
	}

	dt := d24
	s.ex = false
	if d24 == 0x00FFFFFF {
		ext, err := r.readU32BE()
		if err != nil {
			return err
		}
		dt = ext
		s.ex = true
	}

	// fmt1 timestamp is previous absolute timestamp + delta.
	s.ts += dt
	s.dt = dt
	s.ml = ml
	s.mt = mt
	s.ok = true
	return nil
}

func (r *Reader) readFmt2(s *chunkStreamState) error {
	d24, err := r.readU24BE()
	if err != nil {
		return err
	}

	dt := d24
	s.ex = false
	if d24 == 0x00FFFFFF {
		ext, err := r.readU32BE()
		if err != nil {
			return err
		}
		dt = ext
		s.ex = true
	}

	// fmt2 timestamp is previous absolute timestamp + delta.
	s.ts += dt
	s.dt = dt
	s.ok = true
	return nil
}

func (r *Reader) readU8() (uint8, error) {
	b, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	return b, nil
}

// We use ReadByte one by one here, and we don't use a byte array like `var b [3]byte` and `io.ReadFull`.
// Passing array to io.ReadFull makes it escape to heap memory, which we avoid here.
func (r *Reader) readU24BE() (uint32, error) {
	b0, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	b1, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	b2, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	return uint32(b0)<<16 | uint32(b1)<<8 | uint32(b2), nil
}

func (r *Reader) readU32BE() (uint32, error) {
	b0, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	b1, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	b2, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	b3, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	return uint32(b0)<<24 | uint32(b1)<<16 | uint32(b2)<<8 | uint32(b3), nil
}

func (r *Reader) readU32LE() (uint32, error) {
	b0, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	b1, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	b2, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	b3, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	return uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16 | uint32(b3)<<24, nil
}

func minU32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}
