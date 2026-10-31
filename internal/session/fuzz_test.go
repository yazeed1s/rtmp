package session

import (
	"testing"

	"github.com/yazeed1s/rtmp/internal/fuzzbound"
)

func FuzzParseEnhancedVideo(f *testing.F) {
	f.Add([]byte{0x90, 'h', 'v', 'c', '1', 1, 2, 3})
	f.Add([]byte{0x96, 0x00, 'a', 'v', 'c', '1', 0, 0, 0, 3, 1, 2, 3})
	f.Fuzz(func(t *testing.T, b []byte) {
		fuzzbound.Check(t, b, func() { _, _ = parseEnhancedVideo(b) })
	})
}

func FuzzParseEnhancedAudio(f *testing.F) {
	f.Add([]byte{0x90, 'O', 'p', 'u', 's', 1, 2})
	f.Add([]byte{0x95, 0x00, 'O', 'p', 'u', 's', 0, 0, 1, 9})
	f.Fuzz(func(t *testing.T, b []byte) {
		fuzzbound.Check(t, b, func() { _, _ = parseEnhancedAudio(b) })
	})
}
