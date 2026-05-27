package chunk

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
)

func TestReadFmt0(t *testing.T) {
	b := makeFmt0(25, 0x010203, 4, 0x14, 0x01020304, []byte{1, 2, 3, 4}, false)
	r := NewReader(bufio.NewReader(bytes.NewReader(b)))

	h, p, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if h.CSID != 25 {
		t.Fatalf("csid mismatch: got=%d want=25", h.CSID)
	}
	if h.Timestamp != 0x010203 {
		t.Fatalf("timestamp mismatch: got=%d want=%d", h.Timestamp, uint32(0x010203))
	}
	if h.MessageLength != 4 {
		t.Fatalf("message len mismatch: got=%d want=4", h.MessageLength)
	}
	if h.MessageTypeID != 0x14 {
		t.Fatalf("message type mismatch: got=0x%02x want=0x14", h.MessageTypeID)
	}
	if h.MessageStreamID != 0x01020304 {
		t.Fatalf("stream id mismatch: got=0x%08x want=0x01020304", h.MessageStreamID)
	}
	if !h.IsNewMessage {
		t.Fatal("expected IsNewMessage true")
	}
	if !bytes.Equal(p, []byte{1, 2, 3, 4}) {
		t.Fatalf("payload mismatch: got=%x", p)
	}
}

func TestReadFmt0ExtendedTimestamp(t *testing.T) {
	b := makeFmt0(25, 0x10203040, 2, 0x09, 1, []byte{9, 8}, true)
	r := NewReader(bufio.NewReader(bytes.NewReader(b)))

	h, p, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if h.Timestamp != 0x10203040 {
		t.Fatalf("timestamp mismatch: got=0x%08x want=0x10203040", h.Timestamp)
	}
	if !bytes.Equal(p, []byte{9, 8}) {
		t.Fatalf("payload mismatch: got=%x", p)
	}
}

func TestReadFmt1AndFmt2(t *testing.T) {
	var s []byte
	s = append(s, makeFmt0(25, 100, 2, 0x08, 1, []byte{1, 2}, false)...)
	s = append(s, makeFmt1(25, 50, 3, 0x08, []byte{3, 4, 5}, false)...)
	s = append(s, makeFmt2(25, 30, []byte{6, 7, 8}, false)...)

	r := NewReader(bufio.NewReader(bytes.NewReader(s)))

	if _, _, err := r.ReadChunk(); err != nil {
		t.Fatalf("read 1: %v", err)
	}

	h2, p2, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if h2.Timestamp != 150 {
		t.Fatalf("timestamp mismatch: got=%d want=150", h2.Timestamp)
	}
	if h2.MessageLength != 3 {
		t.Fatalf("message len mismatch: got=%d want=3", h2.MessageLength)
	}
	if !h2.IsNewMessage {
		t.Fatal("expected IsNewMessage true")
	}
	if !bytes.Equal(p2, []byte{3, 4, 5}) {
		t.Fatalf("payload mismatch: got=%x", p2)
	}

	h3, p3, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read 3: %v", err)
	}
	if h3.Timestamp != 180 {
		t.Fatalf("timestamp mismatch: got=%d want=180", h3.Timestamp)
	}
	if h3.MessageLength != 3 {
		t.Fatalf("message len mismatch: got=%d want=3", h3.MessageLength)
	}
	if !h3.IsNewMessage {
		t.Fatal("expected IsNewMessage true")
	}
	if !bytes.Equal(p3, []byte{6, 7, 8}) {
		t.Fatalf("payload mismatch: got=%x", p3)
	}
}

func TestReadFmt3ContinuationAndChunkSize(t *testing.T) {
	var s []byte
	s = append(s, makeFmt0(25, 1000, 5, 0x09, 1, []byte{1, 2, 3}, false)...)
	s = append(s, makeFmt3(25, nil, []byte{4, 5})...)

	r := NewReader(bufio.NewReader(bytes.NewReader(s)))
	r.SetChunkSize(3)

	h1, p1, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if !h1.IsNewMessage {
		t.Fatal("expected first chunk new message")
	}
	if !bytes.Equal(p1, []byte{1, 2, 3}) {
		t.Fatalf("payload1 mismatch: got=%x", p1)
	}

	h2, p2, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if h2.IsNewMessage {
		t.Fatal("expected continuation chunk")
	}
	if !bytes.Equal(p2, []byte{4, 5}) {
		t.Fatalf("payload2 mismatch: got=%x", p2)
	}
}

func TestReadFmt3NewMessage(t *testing.T) {
	var s []byte
	s = append(s, makeFmt0(25, 100, 2, 0x08, 1, []byte{1, 2}, false)...)
	s = append(s, makeFmt2(25, 20, []byte{3, 4}, false)...)
	s = append(s, makeFmt3(25, nil, []byte{5, 6})...)

	r := NewReader(bufio.NewReader(bytes.NewReader(s)))

	if _, _, err := r.ReadChunk(); err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if _, _, err := r.ReadChunk(); err != nil {
		t.Fatalf("read 2: %v", err)
	}
	h3, p3, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read 3: %v", err)
	}

	if !h3.IsNewMessage {
		t.Fatal("expected fmt3 to start new message")
	}
	if h3.Timestamp != 140 {
		t.Fatalf("timestamp mismatch: got=%d want=140", h3.Timestamp)
	}
	if !bytes.Equal(p3, []byte{5, 6}) {
		t.Fatalf("payload mismatch: got=%x", p3)
	}
}

func TestReadFmt3WithExtendedTimestampField(t *testing.T) {
	var s []byte
	s = append(s, makeFmt0(25, 0x11223344, 3, 0x09, 1, []byte{1, 2}, true)...)
	s = append(s, makeFmt3(25, u32be(0x11223344), []byte{3})...)

	r := NewReader(bufio.NewReader(bytes.NewReader(s)))
	r.SetChunkSize(2)

	if _, _, err := r.ReadChunk(); err != nil {
		t.Fatalf("read 1: %v", err)
	}
	h2, p2, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if h2.IsNewMessage {
		t.Fatal("expected continuation")
	}
	if !bytes.Equal(p2, []byte{3}) {
		t.Fatalf("payload mismatch: got=%x", p2)
	}
}

func TestReadBasicHeader2ByteCSID(t *testing.T) {
	b := makeFmt0(70, 1, 1, 0x14, 1, []byte{0xAA}, false)
	r := NewReader(bufio.NewReader(bytes.NewReader(b)))
	h, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if h.CSID != 70 {
		t.Fatalf("csid mismatch: got=%d want=70", h.CSID)
	}
}

func TestReadBasicHeader3ByteCSID(t *testing.T) {
	b := makeFmt0(500, 1, 1, 0x14, 1, []byte{0xAA}, false)
	r := NewReader(bufio.NewReader(bytes.NewReader(b)))
	h, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if h.CSID != 500 {
		t.Fatalf("csid mismatch: got=%d want=500", h.CSID)
	}
}

func TestReadStreamIDLittleEndian(t *testing.T) {
	b := makeFmt0(25, 1, 1, 0x14, 1, []byte{0xAA}, false)
	r := NewReader(bufio.NewReader(bytes.NewReader(b)))

	h, _, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if h.MessageStreamID != 1 {
		t.Fatalf("stream id mismatch: got=%d want=1", h.MessageStreamID)
	}
}

func TestWriterRoundTripWithReader(t *testing.T) {
	var bf bytes.Buffer
	w := NewWriter(bufio.NewWriter(&bf))

	in := []byte{0x01, 0x02, 0x03, 0x04}
	if err := w.WriteCommand(in); err != nil {
		t.Fatalf("write command: %v", err)
	}

	r := NewReader(bufio.NewReader(bytes.NewReader(bf.Bytes())))
	r.SetChunkSize(4096)

	h, p, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if h.CSID != 3 {
		t.Fatalf("csid mismatch: got=%d want=3", h.CSID)
	}
	if h.MessageTypeID != 0x14 {
		t.Fatalf("type mismatch: got=0x%02x want=0x14", h.MessageTypeID)
	}
	if h.MessageStreamID != 0 {
		t.Fatalf("stream id mismatch: got=%d want=0", h.MessageStreamID)
	}
	if !h.IsNewMessage {
		t.Fatal("expected new message")
	}
	if !bytes.Equal(p, in) {
		t.Fatalf("payload mismatch: got=%x want=%x", p, in)
	}
}

func TestWriterMultiChunkFmt3(t *testing.T) {
	var bf bytes.Buffer
	w := NewWriter(bufio.NewWriter(&bf))
	w.SetChunkSize(4096)

	in := bytes.Repeat([]byte{0xAB}, 8192)
	if err := w.WriteMessage(6, 0x09, 1, 0, in); err != nil {
		t.Fatalf("write message: %v", err)
	}

	raw := bf.Bytes()
	// First chunk: 1 basic + 11 fmt0 header + 4096 payload.
	off := 1 + 11 + 4096
	if off >= len(raw) {
		t.Fatalf("raw too short: len=%d off=%d", len(raw), off)
	}
	if raw[off] != 0xC6 {
		t.Fatalf("expected fmt3 basic header 0xC6, got=0x%02x", raw[off])
	}

	r := NewReader(bufio.NewReader(bytes.NewReader(raw)))
	r.SetChunkSize(4096)

	h1, p1, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read chunk1: %v", err)
	}
	if !h1.IsNewMessage {
		t.Fatal("chunk1 should be new message")
	}
	if len(p1) != 4096 {
		t.Fatalf("chunk1 payload len mismatch: got=%d want=4096", len(p1))
	}

	h2, p2, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("read chunk2: %v", err)
	}
	if h2.IsNewMessage {
		t.Fatal("chunk2 should be continuation")
	}
	if len(p2) != 4096 {
		t.Fatalf("chunk2 payload len mismatch: got=%d want=4096", len(p2))
	}
}

func TestWriterLittleEndianStreamID(t *testing.T) {
	var bf bytes.Buffer
	w := NewWriter(bufio.NewWriter(&bf))

	if err := w.WriteMessage(3, 0x14, 1, 0, []byte{0xAA}); err != nil {
		t.Fatalf("write message: %v", err)
	}

	raw := bf.Bytes()
	if len(raw) < 12 {
		t.Fatalf("raw too short: %d", len(raw))
	}
	// Stream id starts after: basic(1) + ts(3) + len(3) + type(1) = offset 8.
	sid := raw[8:12]
	ex := []byte{0x01, 0x00, 0x00, 0x00}
	if !bytes.Equal(sid, ex) {
		t.Fatalf("stream id bytes mismatch: got=%x want=%x", sid, ex)
	}
}

func makeFmt0(csid uint32, ts uint32, ml uint32, mt uint8, ms uint32, p []byte, ex bool) []byte {
	out := make([]byte, 0, 32+len(p))
	out = append(out, basic(0, csid)...)
	if ex {
		out = append(out, 0xFF, 0xFF, 0xFF)
	} else {
		out = append(out, u24be(ts)...)
	}
	out = append(out, u24be(ml)...)
	out = append(out, mt)
	out = append(out, u32le(ms)...)
	if ex {
		out = append(out, u32be(ts)...)
	}
	out = append(out, p...)
	return out
}

func makeFmt1(csid uint32, dt uint32, ml uint32, mt uint8, p []byte, ex bool) []byte {
	out := make([]byte, 0, 24+len(p))
	out = append(out, basic(1, csid)...)
	if ex {
		out = append(out, 0xFF, 0xFF, 0xFF)
	} else {
		out = append(out, u24be(dt)...)
	}
	out = append(out, u24be(ml)...)
	out = append(out, mt)
	if ex {
		out = append(out, u32be(dt)...)
	}
	out = append(out, p...)
	return out
}

func makeFmt2(csid uint32, dt uint32, p []byte, ex bool) []byte {
	out := make([]byte, 0, 16+len(p))
	out = append(out, basic(2, csid)...)
	if ex {
		out = append(out, 0xFF, 0xFF, 0xFF)
		out = append(out, u32be(dt)...)
	} else {
		out = append(out, u24be(dt)...)
	}
	out = append(out, p...)
	return out
}

func makeFmt3(csid uint32, ext []byte, p []byte) []byte {
	out := make([]byte, 0, 8+len(p))
	out = append(out, basic(3, csid)...)
	out = append(out, ext...)
	out = append(out, p...)
	return out
}

func basic(fm uint8, csid uint32) []byte {
	if csid >= 2 && csid <= 63 {
		return []byte{byte(fm<<6) | byte(csid)}
	}
	if csid >= 64 && csid <= 319 {
		return []byte{byte(fm << 6), byte(csid - 64)}
	}

	v := csid - 64
	return []byte{byte(fm<<6) | 1, byte(v & 0xFF), byte((v >> 8) & 0xFF)}
}

func u24be(v uint32) []byte {
	return []byte{byte(v >> 16), byte(v >> 8), byte(v)}
}

func u32be(v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return b[:]
}

func u32le(v uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return b[:]
}
