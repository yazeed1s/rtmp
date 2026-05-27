package amf0

import "testing"

func FuzzAMF0Decode(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x05}) // null
	f.Add([]byte{0x02, 0x00, 0x03, 'a', 'b', 'c'})
	f.Add([]byte{0x0A, 0x00, 0x00, 0x00, 0x00}) // strict array size 0

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Decode(data)
	})
}
