package control

import "testing"

func TestSetChunkSizeRoundTrip(t *testing.T) {
	b := BuildSetChunkSize(4096)
	v, err := ParseSetChunkSize(b)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if v != 4096 {
		t.Fatalf("value mismatch: got=%d want=4096", v)
	}
}

func TestAbortRoundTrip(t *testing.T) {
	in := uint32(123)
	b := BuildAcknowledgement(in)
	v, err := ParseAbortMessage(b)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if v != in {
		t.Fatalf("value mismatch: got=%d want=%d", v, in)
	}
}

func TestAcknowledgementRoundTrip(t *testing.T) {
	in := uint32(987654)
	b := BuildAcknowledgement(in)
	v, err := ParseAcknowledgement(b)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if v != in {
		t.Fatalf("value mismatch: got=%d want=%d", v, in)
	}
}

func TestWindowAckSizeRoundTrip(t *testing.T) {
	in := uint32(2500000)
	b := BuildWindowAckSize(in)
	v, err := ParseWindowAckSize(b)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if v != in {
		t.Fatalf("value mismatch: got=%d want=%d", v, in)
	}
}

func TestSetPeerBandwidthRoundTrip(t *testing.T) {
	inW := uint32(2500000)
	inL := uint8(2)
	b := BuildSetPeerBandwidth(inW, inL)
	w, l, err := ParseSetPeerBandwidth(b)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if w != inW || l != inL {
		t.Fatalf("value mismatch: got=(%d,%d) want=(%d,%d)", w, l, inW, inL)
	}
}

func TestUserControlRoundTripStreamBegin(t *testing.T) {
	b := BuildUserControlStreamBegin(1)
	ev, d, err := ParseUserControl(b)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if ev != 0 {
		t.Fatalf("event type mismatch: got=%d want=0", ev)
	}
	if len(d) != 4 {
		t.Fatalf("event data len mismatch: got=%d want=4", len(d))
	}
	if d[3] != 1 {
		t.Fatalf("event data mismatch: got=%v want streamID 1", d)
	}
}

func TestPingResponseRoundTrip(t *testing.T) {
	b := BuildPingResponse(0x11223344)
	ev, d, err := ParseUserControl(b)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if ev != 7 {
		t.Fatalf("event type mismatch: got=%d want=7", ev)
	}
	if len(d) != 4 {
		t.Fatalf("event data len mismatch: got=%d want=4", len(d))
	}
	if d[0] != 0x11 || d[1] != 0x22 || d[2] != 0x33 || d[3] != 0x44 {
		t.Fatalf("event data mismatch: got=%v", d)
	}
}

func TestParseSetChunkSizeZero(t *testing.T) {
	_, err := ParseSetChunkSize([]byte{0, 0, 0, 0})
	if err == nil {
		t.Fatal("expected error for size 0")
	}
}

func TestParseSetChunkSizeHighBitMasked(t *testing.T) {
	v, err := ParseSetChunkSize([]byte{0x80, 0x00, 0x00, 0x10})
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if v != 16 {
		t.Fatalf("value mismatch: got=%d want=16", v)
	}
}

func TestParseSetPeerBandwidthBadLimit(t *testing.T) {
	_, _, err := ParseSetPeerBandwidth([]byte{0x00, 0x00, 0x00, 0x01, 0x03})
	if err == nil {
		t.Fatal("expected error for limit type 3")
	}
}

func TestParseUserControlTooShort(t *testing.T) {
	_, _, err := ParseUserControl([]byte{0x00})
	if err == nil {
		t.Fatal("expected error for too-short payload")
	}
}
