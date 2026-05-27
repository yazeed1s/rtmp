package message

import (
	"bytes"
	"testing"

	"github.com/yazeed1s/rtmp/internal/chunk"
	"github.com/yazeed1s/rtmp/internal/pool"
)

func BenchmarkMessageAssemble(b *testing.B) {
	const (
		tl = 100 * 1024
		cs = 128
	)

	dt := bytes.Repeat([]byte{0x5A}, tl)
	hs := make([]chunk.ChunkHeader, 0, (tl+cs-1)/cs)
	ps := make([][]byte, 0, (tl+cs-1)/cs)

	rd := 0
	for rd < tl {
		n := cs
		if (tl - rd) < n {
			n = tl - rd
		}

		hs = append(hs, chunk.ChunkHeader{
			CSID:            6,
			MessageTypeID:   0x09,
			MessageStreamID: 1,
			Timestamp:       0,
			MessageLength:   tl,
			IsNewMessage:    rd == 0,
		})
		ps = append(ps, dt[rd:rd+n])
		rd += n
	}

	pl := pool.New()
	a := NewAssembler(pl, tl+4096)

	b.ReportAllocs()
	b.SetBytes(tl)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var msg *RawMessage
		for j := range hs {
			var err error
			msg, err = a.Feed(hs[j], ps[j])
			if err != nil {
				b.Fatalf("feed: %v", err)
			}
		}

		if msg == nil {
			b.Fatal("want complete message")
		}
		if len(msg.Payload) != tl {
			b.Fatalf("payload len: got=%d want=%d", len(msg.Payload), tl)
		}
		msg.Release()
	}
}
