package rtmp

import (
	"testing"

	"github.com/yazeed1s/rtmp/internal/session"
)

type bh struct{}

func (h bh) OnConnect(sess Session, info ConnectInfo) error { return nil }
func (h bh) OnPublish(sess Session, info PublishInfo) error { return nil }
func (h bh) OnMetadata(sess Session, meta Metadata)         {}
func (h bh) OnPacket(sess Session, pkt *Packet)             {}
func (h bh) OnDisconnect(sess Session, err error)           {}

func BenchmarkOnPacketHotPath(b *testing.B) {
	sh := sessionHandler{h: bh{}}
	p := &session.Packet{
		Type:             session.PacketTypeVideo,
		VideoCodec:       session.VideoCodecH264,
		IsSequenceHeader: false,
		IsKeyframe:       true,
		Timestamp:        12345,
		CompositionTime:  0,
		StreamID:         1,
		Payload:          []byte{0x17, 0x01, 0x65, 0x88, 0x84},
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(p.Payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sh.OnPacket(nil, p)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "pkt/s")
}
