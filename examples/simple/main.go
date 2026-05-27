// Package main is a minimal RTMP ingest server.
// It accepts one publisher and prints what it receives.
//
// Run:
//
//	go run .
//
// Then publish to it:
//
//	ffmpeg -re -i video.mp4 -c copy -f flv rtmp://127.0.0.1:1935/live/testkey
package main

import (
	"log"

	"github.com/yazeed1s/rtmp/rtmp"
)

type handler struct{}

func (handler) OnConnect(s rtmp.Session, info rtmp.ConnectInfo) error {
	log.Printf("[connect] id=%s app=%q remote=%s",
		s.ID(), info.App, s.RemoteAddr())
	return nil
}

func (handler) OnPublish(s rtmp.Session, info rtmp.PublishInfo) error {
	log.Printf("[publish] id=%s key=%q type=%s",
		s.ID(), info.StreamKey, info.Type)
	return nil
}

func (handler) OnMetadata(s rtmp.Session, m rtmp.Metadata) {
	log.Printf("[metadata] id=%s %.0fx%.0f fps=%.1f vcodec=%.0f acodec=%.0f",
		s.ID(), m.Width, m.Height, m.FrameRate, m.VideoCodecID, m.AudioCodecID)
}

func (handler) OnPacket(s rtmp.Session, pkt *rtmp.Packet) {
	kind := "audio"
	if pkt.Type == rtmp.PacketTypeVideo {
		kind = "video"
	}
	log.Printf("[packet] id=%s %s ts=%d len=%d keyframe=%t seq_header=%t",
		s.ID(), kind, pkt.Timestamp, len(pkt.Payload), pkt.IsKeyframe, pkt.IsSequenceHeader)
}

func (handler) OnDisconnect(s rtmp.Session, err error) {
	log.Printf("[disconnect] id=%s err=%v", s.ID(), err)
}

func main() {
	srv, err := rtmp.NewServer(rtmp.Config{
		ListenAddr: ":1935",
	}, handler{})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("listening on :1935")
	log.Fatal(srv.ListenAndServe())
}
