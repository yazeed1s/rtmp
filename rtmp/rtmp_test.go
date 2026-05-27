package rtmp

import "testing"

func TestPacketClone(t *testing.T) {
	og := &Packet{
		Type:       PacketTypeVideo,
		VideoCodec: VideoCodecH264,
		Payload:    []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	}

	cl := og.Clone()
	if cl == nil {
		t.Fatal("clone is nil")
	}

	if len(cl.Payload) != len(og.Payload) {
		t.Fatalf("payload len mismatch: got=%d want=%d", len(cl.Payload), len(og.Payload))
	}

	for i := range og.Payload {
		if cl.Payload[i] != og.Payload[i] {
			t.Fatalf("payload byte mismatch at %d: got=%d want=%d", i, cl.Payload[i], og.Payload[i])
		}
	}

	if &cl.Payload[0] == &og.Payload[0] {
		t.Fatal("clone payload points to original payload")
	}
}

func TestPacketCloneNilPayload(t *testing.T) {
	og := &Packet{
		Type:    PacketTypeAudio,
		Payload: nil,
	}

	cl := og.Clone()
	if cl == nil {
		t.Fatal("clone is nil")
	}

	if cl.Payload != nil {
		t.Fatal("expected nil payload")
	}
}
