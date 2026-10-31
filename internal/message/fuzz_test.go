package message

import (
	"bufio"
	"bytes"
	"errors"
	"testing"

	"github.com/yazeed1s/rtmp/internal/chunk"
	"github.com/yazeed1s/rtmp/internal/fuzzbound"
	"github.com/yazeed1s/rtmp/internal/pool"
)

// TestAssemblerCapsReservedBytes opens a 4 MiB message on many chunk
// streams without finishing any of them.
func TestAssemblerCapsReservedBytes(t *testing.T) {
	a := NewAssembler(pool.New(), 4<<20)
	var err error
	n := 0
	for csid := uint32(3); csid < 1000 && err == nil; csid++ {
		_, err = a.Feed(chunk.ChunkHeader{CSID: csid, MessageTypeID: 9, MessageLength: 4 << 20, IsNewMessage: true}, []byte{1})
		n++
	}
	if !errors.Is(err, ErrMessageTooLarge) || n > 3 {
		t.Fatalf("opened %d partial 4 MiB messages, err=%v", n, err)
	}
	a.Discard(3)
	if _, err := a.Feed(chunk.ChunkHeader{CSID: 900, MessageTypeID: 9, MessageLength: 4 << 20, IsNewMessage: true}, []byte{1}); err != nil {
		t.Fatalf("reservation was not returned on discard: %v", err)
	}
}

// fuzzPool is shared by all fuzz inputs, the same as one long connection.
// A new pool per input would only measure sync.Pool churn, not the parser.
var fuzzPool = pool.New()

// FuzzChunkStream reads chunks from arbitrary bytes into the assembler, the
// same path an RTMP connection takes after the handshake.
func FuzzChunkStream(f *testing.F) {
	f.Add([]byte{0x03, 0, 0, 0, 0, 0, 5, 0x14, 0, 0, 0, 0, 1, 2, 3, 4, 5})
	f.Add([]byte{0x02, 0, 0, 0, 0, 0, 4, 0x01, 0, 0, 0, 0, 0, 0, 0x10, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		fuzzbound.Check(t, b, func() {
			rd := chunk.NewReader(bufio.NewReader(bytes.NewReader(b)))
			a := NewAssembler(fuzzPool, 1<<20)
			for i := 0; i < 256; i++ {
				h, n, err := rd.ReadChunkHeader()
				if err != nil {
					return
				}
				m, err := a.FeedRead(h, n, rd.ReadChunkBody)
				if err != nil {
					return
				}
				if m != nil {
					if len(m.Payload) > 1<<20 {
						t.Fatalf("message of %d bytes passed a 1 MiB limit", len(m.Payload))
					}
					m.Release()
				}
			}
		})
	})
}
