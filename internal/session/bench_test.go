package session

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/yazeed1s/rtmp/internal/chunk"
	"github.com/yazeed1s/rtmp/internal/message"
	"github.com/yazeed1s/rtmp/internal/pool"
)

func BenchmarkOwnershipChunkToMessage(b *testing.B) {
	const (
		nm = 500
		pl = 1024
		cs = 128
	)

	p := bytes.Repeat([]byte{0xAB}, pl)
	var bf bytes.Buffer
	w := chunk.NewWriter(bufio.NewWriter(&bf))
	w.SetChunkSize(cs)

	for i := 0; i < nm; i++ {
		if err := w.WriteMessage(6, 0x09, 1, uint32(i*33), p); err != nil {
			b.Fatalf("build chunk stream: %v", err)
		}
	}

	raw := bf.Bytes()
	pll := pool.New()

	b.ReportAllocs()
	b.SetBytes(int64(nm * pl))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := chunk.NewReader(bufio.NewReader(bytes.NewReader(raw)))
		r.SetChunkSize(cs)
		a := message.NewAssembler(pll, 4*1024*1024)
		rd := r.ReadChunkBody

		gm := 0
		for gm < nm {
			h, n, err := r.ReadChunkHeader()
			if err != nil {
				b.Fatalf("read chunk header: %v", err)
			}

			m, err := a.FeedRead(h, n, rd)
			if err != nil {
				b.Fatalf("feed read: %v", err)
			}
			if m == nil {
				continue
			}

			if len(m.Payload) != pl {
				b.Fatalf("payload len: got=%d want=%d", len(m.Payload), pl)
			}
			m.Release()
			gm++
		}
	}
}
