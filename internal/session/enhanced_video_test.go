package session

import (
	"encoding/binary"
	"testing"
)

func TestParseEnhancedVideo(t *testing.T) {
	t.Run("SequenceStart_HEVC", func(t *testing.T) {
		buf := []byte{0x90}
		fcc := make([]byte, 4)
		binary.BigEndian.PutUint32(fcc, uint32(FourCCHEVC))
		buf = append(buf, fcc...)
		buf = append(buf, []byte{1, 2, 3}...)

		pkt, err := parseEnhancedVideo(buf)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if pkt.FourCC != FourCCHEVC || pkt.VideoPacketType != VideoPacketSequenceStart || !pkt.IsSequenceHeader || !pkt.IsKeyframe {
			t.Errorf("mismatch: %+v", pkt)
		}
	})

	t.Run("CodedFrames_AV1", func(t *testing.T) {
		buf := []byte{0x91}
		fcc := make([]byte, 4)
		binary.BigEndian.PutUint32(fcc, uint32(FourCCAV1))
		buf = append(buf, fcc...)
		buf = append(buf, []byte{0x00, 0x00, 0x10}...)
		buf = append(buf, []byte{9, 9, 9}...)

		pkt, err := parseEnhancedVideo(buf)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if pkt.FourCC != FourCCAV1 || pkt.VideoPacketType != VideoPacketCodedFrames || pkt.CompositionTime != 16 || !pkt.IsKeyframe {
			t.Errorf("mismatch: %+v", pkt)
		}
	})

	t.Run("CodedFramesX_VP9", func(t *testing.T) {
		buf := []byte{0x93}
		fcc := make([]byte, 4)
		binary.BigEndian.PutUint32(fcc, uint32(FourCCVP9))
		buf = append(buf, fcc...)
		buf = append(buf, []byte{5, 5, 5}...)

		pkt, err := parseEnhancedVideo(buf)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if pkt.FourCC != FourCCVP9 || pkt.VideoPacketType != VideoPacketCodedFramesX || pkt.CompositionTime != 0 {
			t.Errorf("mismatch: %+v", pkt)
		}
	})

	t.Run("SequenceEnd_AVC", func(t *testing.T) {
		buf := []byte{0x92}
		fcc := make([]byte, 4)
		binary.BigEndian.PutUint32(fcc, uint32(FourCCAVC))
		buf = append(buf, fcc...)

		pkt, err := parseEnhancedVideo(buf)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if pkt.VideoPacketType != VideoPacketSequenceEnd || len(pkt.Payload) != 0 {
			t.Errorf("mismatch: %+v", pkt)
		}
	})

	t.Run("ShortPayload", func(t *testing.T) {
		if _, err := parseEnhancedVideo([]byte{0x90, 0x01}); err == nil {
			t.Error("expected error for short payload")
		}
	})

	t.Run("UnknownFourCC", func(t *testing.T) {
		buf := []byte{0x91}
		buf = append(buf, []byte("ABCD")...)
		buf = append(buf, []byte{0, 0, 0, 1}...)
		pkt, err := parseEnhancedVideo(buf)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if pkt.FourCC != FourCC('A'<<24|'B'<<16|'C'<<8|'D') {
			t.Errorf("expected ABCD: %v", pkt.FourCC)
		}
	})
}
