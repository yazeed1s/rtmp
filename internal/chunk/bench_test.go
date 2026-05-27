package chunk

import (
	"bufio"
	"bytes"
	"testing"
)

func BenchmarkChunkRead(b *testing.B) {
	const (
		nm = 1000
		pl = 128
	)

	p := bytes.Repeat([]byte{0xAB}, pl)
	var bf bytes.Buffer
	for i := 0; i < nm; i++ {
		c := mkFmt0(3, uint32(i*33), uint32(len(p)), 0x09, 1, p)
		_, _ = bf.Write(c)
	}
	raw := bf.Bytes()

	b.ReportAllocs()
	b.SetBytes(int64(nm * pl))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := NewReader(bufio.NewReader(bytes.NewReader(raw)))
		r.SetChunkSize(pl)

		for j := 0; j < nm; j++ {
			h, p2, err := r.ReadChunk()
			if err != nil {
				b.Fatalf("read chunk: %v", err)
			}
			if h.MessageLength != pl {
				b.Fatalf("message len: got=%d want=%d", h.MessageLength, pl)
			}
			if len(p2) != pl {
				b.Fatalf("payload len: got=%d want=%d", len(p2), pl)
			}
		}
	}
}

func mkFmt0(cs uint32, ts uint32, ml uint32, mt uint8, ms uint32, p []byte) []byte {
	o := make([]byte, 0, 32+len(p))
	o = append(o, mkBh0(cs)...)
	o = append(o, byte(ts>>16), byte(ts>>8), byte(ts))
	o = append(o, byte(ml>>16), byte(ml>>8), byte(ml))
	o = append(o, mt)
	// Spec detail: stream id in fmt0 header is little-endian.
	o = append(o, byte(ms), byte(ms>>8), byte(ms>>16), byte(ms>>24))
	o = append(o, p...)
	return o
}

func mkBh0(cs uint32) []byte {
	// For this benchmark we only use csid=3.
	// That is one-byte basic header form.
	return []byte{byte(cs)}
}
