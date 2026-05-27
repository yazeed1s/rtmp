package message

import (
	"testing"

	"github.com/yazeed1s/rtmp/internal/chunk"
	"github.com/yazeed1s/rtmp/internal/pool"
)

func FuzzAssembler(f *testing.F) {
	f.Add([]byte{0x11, 0x22, 0x33, 0x44})
	f.Add([]byte{0x01})
	f.Add([]byte{})

	pl := pool.New()

	f.Fuzz(func(t *testing.T, data []byte) {
		// Reuse one pool across fuzz cases.
		// New pool per case allocates too much and can kill fuzz process.
		a := NewAssembler(pl, 512)
		if len(data) == 0 {
			return
		}

		cs := uint32(data[0]%8 + 2)
		ml := uint32(len(data)%64 + 1)
		h := chunk.ChunkHeader{
			CSID:            cs,
			MessageTypeID:   uint8(data[0]),
			MessageStreamID: 1,
			Timestamp:       uint32(len(data)),
			MessageLength:   ml,
			IsNewMessage:    true,
		}

		p := data[1:]
		if len(p) == 0 {
			p = []byte{0x00}
		}
		if len(p) > int(ml) {
			p = p[:ml]
		}

		msg, err := a.Feed(h, p)
		if msg != nil {
			msg.Release()
		}
		if err != nil {
			return
		}

		rd := uint32(len(p))
		for rd < ml {
			h2 := h
			h2.IsNewMessage = false

			n := int(ml - rd)
			if n > len(data) {
				n = len(data)
			}
			if n == 0 {
				return
			}

			msg, err := a.Feed(h2, data[:n])
			if msg != nil {
				msg.Release()
			}
			if err != nil {
				return
			}

			rd += uint32(n)
		}

		if len(data)%2 == 0 {
			a.Discard(cs)
		}
	})
}
