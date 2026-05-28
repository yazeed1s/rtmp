package session

import (
	"encoding/binary"
	"fmt"
)

// parseEnhancedAudio parses an enhanced audio tag payload.
// The caller already confirmed byte 0 upper 4 bits are 9.
//
// Wire format:
//
//	byte 0: [audioCodecID:4][audioPacketType:4]
//	         when audioCodecID == 9 it's enhanced audio
//
//	After optional ModEx chain:
//	  bytes N..N+3: FourCC (big-endian ASCII)
//	  bytes N+4..:  audio data
func parseEnhancedAudio(buf []byte) (Packet, error) {
	var pkt Packet
	pkt.Type = PacketTypeAudio
	pkt.IsEnhanced = true

	if len(buf) < 5 {
		return pkt, fmt.Errorf("enhanced audio: too short (%d)", len(buf))
	}

	cid := (buf[0] >> 4) & 0x0F
	if cid != 9 {
		return pkt, fmt.Errorf("enhanced audio: invalid codec id %d", cid)
	}

	pt := AudioPacketType(buf[0] & 0x0F)
	off := 1

	// Skip ModEx prefix chain
	for pt == AudioPacketModEx {
		if off >= len(buf) {
			return pkt, fmt.Errorf("enhanced audio: modex truncated")
		}
		sz := int(buf[off]) + 1
		off++
		if sz == 256 {
			if off+2 > len(buf) {
				return pkt, fmt.Errorf("enhanced audio: modex size truncated")
			}
			sz = int(binary.BigEndian.Uint16(buf[off:])) + 1
			off += 2
		}
		if off+sz > len(buf) {
			return pkt, fmt.Errorf("enhanced audio: modex data truncated")
		}
		off += sz // skip modifier data
		if off >= len(buf) {
			return pkt, fmt.Errorf("enhanced audio: modex next truncated")
		}
		// [modExType:4][packetType:4]
		pt = AudioPacketType(buf[off] & 0x0F)
		off++
	}

	pkt.AudioPacketType = pt

	// Multitrack: inner packetType + FourCC
	if pt == AudioPacketMultitrack {
		return parseMultitrackAudio(buf, off, pkt)
	}

	// Regular enhanced: read FourCC
	if off+4 > len(buf) {
		return pkt, fmt.Errorf("enhanced audio: fourcc truncated")
	}
	pkt.FourCC = FourCC(binary.BigEndian.Uint32(buf[off:]))
	off += 4

	switch pt {
	case AudioPacketSequenceStart:
		pkt.IsSequenceHeader = true
		pkt.Payload = buf[off:]
	case AudioPacketCodedFrames:
		pkt.Payload = buf[off:]
	case AudioPacketSequenceEnd:
		// No payload
	case AudioPacketMultichannelConfig:
		pkt.Payload = buf[off:]
	default:
		// Unknown type, just emit raw
		pkt.Payload = buf[off:]
	}

	return pkt, nil
}

// parseMultitrackAudio handles the Multitrack envelope for audio.
func parseMultitrackAudio(buf []byte, off int, pkt Packet) (Packet, error) {
	if off >= len(buf) {
		return pkt, fmt.Errorf("enhanced audio: multitrack header truncated")
	}

	mt := (buf[off] >> 4) & 0x0F
	off++

	// ManyTracksManyCodecs = 2 -> per-track FourCC
	if mt != 2 && off+4 <= len(buf) {
		pkt.FourCC = FourCC(binary.BigEndian.Uint32(buf[off:]))
	}

	pkt.Payload = buf[off:]
	return pkt, nil
}
