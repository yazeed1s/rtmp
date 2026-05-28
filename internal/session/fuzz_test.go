package session

import "testing"

func FuzzParseEnhancedVideo(f *testing.F) {
	f.Add([]byte{0x90, 'h', 'v', 'c', '1', 0x01, 0x02, 0x03})
	f.Add([]byte{0x91, 'a', 'v', '0', '1', 0x00, 0x00, 0x10, 0x99})
	f.Add([]byte{0x92, 'a', 'v', 'c', '1'})
	f.Fuzz(func(t *testing.T, data []byte) {
		// make sure it doesn't panic on malformed input
		_, _ = parseEnhancedVideo(data)
	})
}

func FuzzParseEnhancedAudio(f *testing.F) {
	f.Add([]byte{0x90, 'O', 'p', 'u', 's', 0x00, 0x00})
	f.Add([]byte{0x91, 'O', 'p', 'u', 's', 0x11, 0x22})
	f.Fuzz(func(t *testing.T, data []byte) {
		// same as above, but for audio packets.
		if len(data) > 0 && (data[0]>>4)&0x0F == 9 {
			_, _ = parseEnhancedAudio(data)
		}
	})
}
