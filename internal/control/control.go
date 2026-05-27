package control

import (
	"encoding/binary"
	"fmt"
)

func ParseSetChunkSize(p []byte) (uint32, error) {
	if len(p) != 4 {
		return 0, fmt.Errorf("control: set chunk size payload len must be 4")
	}
	v := binary.BigEndian.Uint32(p) & 0x7FFFFFFF
	if v < 1 {
		return 0, fmt.Errorf("control: chunk size must be >= 1")
	}
	return v, nil
}

func ParseAbortMessage(p []byte) (uint32, error) {
	if len(p) != 4 {
		return 0, fmt.Errorf("control: abort payload len must be 4")
	}
	return binary.BigEndian.Uint32(p), nil
}

func ParseAcknowledgement(p []byte) (uint32, error) {
	if len(p) != 4 {
		return 0, fmt.Errorf("control: acknowledgement payload len must be 4")
	}
	return binary.BigEndian.Uint32(p), nil
}

func ParseWindowAckSize(p []byte) (uint32, error) {
	if len(p) != 4 {
		return 0, fmt.Errorf("control: window ack size payload len must be 4")
	}
	return binary.BigEndian.Uint32(p), nil
}

func ParseSetPeerBandwidth(p []byte) (uint32, uint8, error) {
	if len(p) != 5 {
		return 0, 0, fmt.Errorf("control: set peer bandwidth payload len must be 5")
	}
	w := binary.BigEndian.Uint32(p[:4])
	lt := p[4]
	if lt > 2 {
		return 0, 0, fmt.Errorf("control: invalid peer bandwidth limit type: %d", lt)
	}
	return w, lt, nil
}

func ParseUserControl(p []byte) (uint16, []byte, error) {
	if len(p) < 2 {
		return 0, nil, fmt.Errorf("control: user control payload len must be >= 2")
	}
	ev := binary.BigEndian.Uint16(p[:2])
	return ev, p[2:], nil
}

func BuildWindowAckSize(sz uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, sz)
	return b
}

func BuildSetPeerBandwidth(sz uint32, lt uint8) []byte {
	b := make([]byte, 5)
	binary.BigEndian.PutUint32(b[:4], sz)
	b[4] = lt
	return b
}

func BuildSetChunkSize(sz uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, sz&0x7FFFFFFF)
	return b
}

func BuildAcknowledgement(sn uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, sn)
	return b
}

func BuildUserControlStreamBegin(sid uint32) []byte {
	b := make([]byte, 6)
	binary.BigEndian.PutUint16(b[:2], 0x0000)
	binary.BigEndian.PutUint32(b[2:], sid)
	return b
}

func BuildPingResponse(ts uint32) []byte {
	b := make([]byte, 6)
	binary.BigEndian.PutUint16(b[:2], 0x0007)
	binary.BigEndian.PutUint32(b[2:], ts)
	return b
}
