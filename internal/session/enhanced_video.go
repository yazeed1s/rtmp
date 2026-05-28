package session

import (
	"encoding/binary"
	"fmt"
)

// parseEnhancedVideo parses an enhanced video tag payload.
// The caller already confirmed byte 0 bit 7 is set (IsExHeader).
//
// Wire format:
//
//	byte 0: [1][frameType:3][packetType:4]
//	              ^-- 1=key, 2=inter, 3=disposable, 4=generated, 5=command
//
//	After optional ModEx chain:
//	  bytes N..N+3: FourCC (big-endian)
//	  bytes N+4..:  body (depends on packetType)
func parseEnhancedVideo(buf []byte) (Packet, error) {
	var pkt Packet
	pkt.Type = PacketTypeVideo
	pkt.IsEnhanced = true

	if len(buf) < 5 {
		return pkt, fmt.Errorf("enhanced video: too short (%d)", len(buf))
	}

	ft := (buf[0] >> 4) & 0x07
	pt := VideoPacketType(buf[0] & 0x0F)
	off := 1

	pkt.IsKeyframe = ft == 1 || ft == 4

	// Skip ModEx prefix chain
	// Each ModEx carries a small modifier blob, then a new
	// [modExType:4][packetType:4] byte before the real header.
	for pt == VideoPacketModEx {
		if off >= len(buf) {
			return pkt, fmt.Errorf("enhanced video: modex truncated")
		}
		sz := int(buf[off]) + 1
		off++
		if sz == 256 {
			if off+2 > len(buf) {
				return pkt, fmt.Errorf("enhanced video: modex size truncated")
			}
			sz = int(binary.BigEndian.Uint16(buf[off:])) + 1
			off += 2
		}
		if off+sz > len(buf) {
			return pkt, fmt.Errorf("enhanced video: modex data truncated")
		}
		off += sz // skip modifier data
		if off >= len(buf) {
			return pkt, fmt.Errorf("enhanced video: modex next truncated")
		}
		// [modExType:4][packetType:4]
		pt = VideoPacketType(buf[off] & 0x0F)
		off++
	}

	pkt.VideoPacketType = pt

	// Command frame?
	if pt != VideoPacketMetadata && ft == 5 {
		if off < len(buf) {
			pkt.Payload = buf[off:]
		}
		return pkt, nil
	}

	// Multitrack own header layout with inner packetType + FourCC.
	if pt == VideoPacketMultitrack {
		return parseMultitrackVideo(buf, off, ft, pkt)
	}

	// Regular enhanced read FourCC.
	if off+4 > len(buf) {
		return pkt, fmt.Errorf("enhanced video: fourcc truncated")
	}
	pkt.FourCC = FourCC(binary.BigEndian.Uint32(buf[off:]))
	off += 4

	switch pt {
	case VideoPacketSequenceStart:
		pkt.IsSequenceHeader = true
		pkt.Payload = buf[off:]

	case VideoPacketCodedFrames:
		// SI24 composition time offset after the FourCC.
		if off+3 > len(buf) {
			return pkt, fmt.Errorf("enhanced video: composition time truncated")
		}
		ct := int32(buf[off])<<16 | int32(buf[off+1])<<8 | int32(buf[off+2])
		if ct >= 0x800000 {
			ct -= 0x1000000 // sign-extend 24→32
		}
		pkt.CompositionTime = ct
		off += 3
		pkt.Payload = buf[off:]

	case VideoPacketCodedFramesX:
		// Composition time is implicitly zero.
		pkt.Payload = buf[off:]

	case VideoPacketSequenceEnd:
		// No body.

	case VideoPacketMetadata:
		pkt.Payload = buf[off:]

	case VideoPacketMPEG2TSSeqStart:
		pkt.IsSequenceHeader = true
		pkt.Payload = buf[off:]

	default:
		// Unknown future packet type, just skip it for now
		pkt.Payload = buf[off:]
	}

	return pkt, nil
}

// parseMultitrackVideo handles the Multitrack envelope.
// Full per-track iteration is deferred; we extract the shared
// FourCC (if not ManyTracksManyCodecs) and pass the raw body.
//
//	byte off:   [multitrackType:4][innerPacketType:4]
//	byte off+1: FourCC (4 bytes) only if multitrackType != 2
//	rest:       raw track data
func parseMultitrackVideo(buf []byte, off int, ft uint8, pkt Packet) (Packet, error) {
	if off >= len(buf) {
		return pkt, fmt.Errorf("enhanced video: multitrack header truncated")
	}

	mt := (buf[off] >> 4) & 0x0F // AvMultitrackType
	off++

	// ManyTracksManyCodecs = 2 → per-track FourCC
	if mt != 2 && off+4 <= len(buf) {
		pkt.FourCC = FourCC(binary.BigEndian.Uint32(buf[off:]))
	}

	// Expose raw multitrack body. Full track iteration is not
	// done here, prism or the handler can parse if needed.
	pkt.Payload = buf[off:]
	return pkt, nil
}
