package message

import (
	"errors"
	"testing"

	"github.com/yazeed1s/rtmp/internal/chunk"
	"github.com/yazeed1s/rtmp/internal/pool"
)

func TestAssemblerSingleChunk(t *testing.T) {
	a := NewAssembler(pool.New(), 1024)
	h := chunk.ChunkHeader{
		CSID:            3,
		MessageTypeID:   0x14,
		MessageStreamID: 1,
		Timestamp:       123,
		MessageLength:   4,
		IsNewMessage:    true,
	}

	m, err := a.Feed(h, []byte{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("feed error: %v", err)
	}
	if m == nil {
		t.Fatal("expected message")
	}
	if m.TypeID != 0x14 || m.StreamID != 1 || m.Timestamp != 123 {
		t.Fatalf("message header mismatch: %#v", m)
	}
	if len(m.Payload) != 4 {
		t.Fatalf("payload len mismatch: got=%d want=4", len(m.Payload))
	}
	m.Release()
}

func TestAssemblerMultiChunk(t *testing.T) {
	a := NewAssembler(pool.New(), 1024)
	h0 := chunk.ChunkHeader{
		CSID:            6,
		MessageTypeID:   0x09,
		MessageStreamID: 1,
		Timestamp:       1000,
		MessageLength:   5,
		IsNewMessage:    true,
	}
	h1 := h0
	h1.IsNewMessage = false

	m, err := a.Feed(h0, []byte{1, 2})
	if err != nil {
		t.Fatalf("feed 1 error: %v", err)
	}
	if m != nil {
		t.Fatal("did not expect message yet")
	}

	m, err = a.Feed(h1, []byte{3, 4})
	if err != nil {
		t.Fatalf("feed 2 error: %v", err)
	}
	if m != nil {
		t.Fatal("did not expect message yet")
	}

	m, err = a.Feed(h1, []byte{5})
	if err != nil {
		t.Fatalf("feed 3 error: %v", err)
	}
	if m == nil {
		t.Fatal("expected completed message")
	}
	if got := []byte{1, 2, 3, 4, 5}; string(m.Payload) != string(got) {
		t.Fatalf("payload mismatch: got=%v want=%v", m.Payload, got)
	}
	m.Release()
}

func TestAssemblerMaxSize(t *testing.T) {
	a := NewAssembler(pool.New(), 4)
	h := chunk.ChunkHeader{
		CSID:            4,
		MessageTypeID:   0x08,
		MessageStreamID: 1,
		Timestamp:       10,
		MessageLength:   5,
		IsNewMessage:    true,
	}
	_, err := a.Feed(h, []byte{1})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("expected ErrMessageTooLarge, got=%v", err)
	}
}

func TestAssemblerDiscard(t *testing.T) {
	a := NewAssembler(pool.New(), 1024)
	h := chunk.ChunkHeader{
		CSID:            9,
		MessageTypeID:   0x09,
		MessageStreamID: 1,
		Timestamp:       0,
		MessageLength:   3,
		IsNewMessage:    true,
	}
	_, err := a.Feed(h, []byte{1, 2})
	if err != nil {
		t.Fatalf("feed error: %v", err)
	}
	a.Discard(9)
	if _, ok := a.bf[9]; ok {
		t.Fatal("buffer was not discarded")
	}
}

func TestAssemblerAbandonedPartialOnNewMessage(t *testing.T) {
	a := NewAssembler(pool.New(), 1024)
	h0 := chunk.ChunkHeader{
		CSID:            2,
		MessageTypeID:   0x09,
		MessageStreamID: 1,
		Timestamp:       1,
		MessageLength:   4,
		IsNewMessage:    true,
	}
	_, err := a.Feed(h0, []byte{1, 2})
	if err != nil {
		t.Fatalf("feed old error: %v", err)
	}

	h1 := chunk.ChunkHeader{
		CSID:            2,
		MessageTypeID:   0x09,
		MessageStreamID: 1,
		Timestamp:       2,
		MessageLength:   2,
		IsNewMessage:    true,
	}
	m, err := a.Feed(h1, []byte{8, 9})
	if err != nil {
		t.Fatalf("feed new error: %v", err)
	}
	if m == nil {
		t.Fatal("expected completed message")
	}
	if got := []byte{8, 9}; string(m.Payload) != string(got) {
		t.Fatalf("payload mismatch: got=%v want=%v", m.Payload, got)
	}
	m.Release()
}

func TestAssemblerOverflowChunk(t *testing.T) {
	a := NewAssembler(pool.New(), 1024)
	h := chunk.ChunkHeader{
		CSID:            7,
		MessageTypeID:   0x14,
		MessageStreamID: 1,
		Timestamp:       20,
		MessageLength:   3,
		IsNewMessage:    true,
	}
	_, err := a.Feed(h, []byte{1, 2})
	if err != nil {
		t.Fatalf("feed 1 error: %v", err)
	}

	h.IsNewMessage = false
	_, err = a.Feed(h, []byte{3, 4})
	if err == nil {
		t.Fatal("expected overflow error")
	}
	if !errors.Is(err, ErrBadChunkHeader) {
		t.Fatalf("expected ErrBadChunkHeader, got=%v", err)
	}
}
