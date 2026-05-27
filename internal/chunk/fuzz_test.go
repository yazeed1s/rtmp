package chunk

import (
	"bufio"
	"bytes"
	"testing"
)

func FuzzChunkReader(f *testing.F) {
	// fmt0, csid=3, ts=0, ml=1, type=20, stream=0, payload=0x00
	f.Add([]byte{
		0x03,
		0x00, 0x00, 0x00,
		0x00, 0x00, 0x01,
		0x14,
		0x00, 0x00, 0x00, 0x00,
		0x00,
	})
	// fmt0 with extended timestamp and one payload byte.
	f.Add([]byte{
		0x03,
		0xFF, 0xFF, 0xFF,
		0x00, 0x00, 0x01,
		0x14,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x2A,
		0xAA,
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bufio.NewReader(bytes.NewReader(data)))
		if len(data) > 0 {
			// Keep small chunk sizes to exercise continuation code often.
			r.SetChunkSize(uint32(data[0]%16 + 1))
		}

		for i := 0; i < 100; i++ {
			_, _, err := r.ReadChunk()
			if err != nil {
				return
			}
		}
	})
}
