package session

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestParseEnhancedAudio(t *testing.T) {
	t.Run("SequenceStart_Opus", func(t *testing.T) {
		buf := []byte{0x90}
		fcc := make([]byte, 4)
		binary.BigEndian.PutUint32(fcc, uint32(FourCCOpus))
		buf = append(buf, fcc...)
		buf = append(buf, []byte("config")...)

		pkt, err := parseEnhancedAudio(buf)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if pkt.FourCC != FourCCOpus || pkt.AudioPacketType != AudioPacketSequenceStart || !pkt.IsSequenceHeader {
			t.Errorf("mismatch: %+v", pkt)
		}
		if !bytes.Equal(pkt.Payload, []byte("config")) {
			t.Error("payload mismatch")
		}
	})

	t.Run("CodedFrames_Opus", func(t *testing.T) {
		buf := []byte{0x91}
		fcc := make([]byte, 4)
		binary.BigEndian.PutUint32(fcc, uint32(FourCCOpus))
		buf = append(buf, fcc...)
		buf = append(buf, []byte("audio")...)

		pkt, err := parseEnhancedAudio(buf)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if pkt.AudioPacketType != AudioPacketCodedFrames {
			t.Errorf("mismatch: %+v", pkt)
		}
	})

	t.Run("NonEnhancedID", func(t *testing.T) {
		if _, err := parseEnhancedAudio([]byte{0xA0, 0, 0, 0, 0}); err == nil {
			t.Error("expected err for AAC codec ID")
		}
	})

	t.Run("ShortPayload", func(t *testing.T) {
		if _, err := parseEnhancedAudio([]byte{0x90, 0x01}); err == nil {
			t.Error("expected error for short payload")
		}
	})
}
