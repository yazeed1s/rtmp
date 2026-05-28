// Package main shows how to use this library as the ingest bone
// of a live streaming engine.
//
// This example wires the RTMP library to a per-stream pipeline with:
//   - stream key validation
//   - duplicate publish rejection
//   - async packet forwarding (clone + channel)
//   - backpressure handling (drop or disconnect)
//   - sequence header caching (so late joiners get decoder config)
//   - graceful shutdown
//
// Run:
//
//	go run .
//
// Publish:
//
//	ffmpeg -re -i video.mp4 -c copy -f flv rtmp://127.0.0.1:1935/live/secret123
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/yazeed1s/rtmp/rtmp"
)

// validKeys is a stand-in for your database or auth service.
// In production you would check this against your API.
var validKeys = map[string]bool{
	"secret123": true,
	"testkey":   true,
}

// pipeline represents the downstream processing for one stream.
// In a real engine this would feed your transcoder and transmuxer.
type pipeline struct {
	key string
	ch  chan *rtmp.Packet
	dn  chan struct{}

	mu       sync.Mutex
	videoSeq *rtmp.Packet // cached video sequence header (SPS/PPS)
	audioSeq *rtmp.Packet // cached audio sequence header (AudioSpecificConfig)
}

func newPipeline(key string) *pipeline {
	p := &pipeline{
		key: key,
		ch:  make(chan *rtmp.Packet, 512),
		dn:  make(chan struct{}),
	}
	go p.run()
	return p
}

func (p *pipeline) run() {
	defer close(p.dn)
	for pkt := range p.ch {
		// This is where you would forward to your transcoder.
		// For example:
		//   transcoder.Feed(pkt)
		//
		// The packet is already cloned, so it is safe to hold.
		_ = pkt
	}
	log.Printf("[pipeline] %s stopped", p.key)
}

func (p *pipeline) stop() {
	close(p.ch)
	<-p.dn
}

func (p *pipeline) send(pkt *rtmp.Packet) bool {
	select {
	case p.ch <- pkt:
		return true
	default:
		// Channel full. Downstream is too slow.
		// Some policy: drop the packet, or return false to disconnect.
		return false
	}
}

// registry keeps track of active streams.
// It prevents two publishers from using the same key.
type registry struct {
	mu sync.Mutex
	ss map[string]*pipeline
}

func (r *registry) start(key string) (*pipeline, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.ss[key]; ok {
		return nil, false // already publishing
	}
	p := newPipeline(key)
	r.ss[key] = p
	return p, true
}

func (r *registry) stop(key string) {
	r.mu.Lock()
	p, ok := r.ss[key]
	if ok {
		delete(r.ss, key)
	}
	r.mu.Unlock()
	if p != nil {
		p.stop()
	}
}

func (r *registry) stopAll() {
	r.mu.Lock()
	ss := r.ss
	r.ss = make(map[string]*pipeline)
	r.mu.Unlock()
	for _, p := range ss {
		p.stop()
	}
}

// ingestHandler implements rtmp.Handler and bridges RTMP events
// into the streaming engine's pipeline.
type ingestHandler struct {
	reg *registry
}

func (h *ingestHandler) OnConnect(s rtmp.Session, info rtmp.ConnectInfo) error {
	log.Printf("[ingest] connect id=%s app=%q remote=%s",
		s.ID(), info.App, s.RemoteAddr())

	// You can reject based on app name here.
	// For example, only allow app="live".
	if info.App != "live" {
		log.Printf("[ingest] rejected: bad app name %q", info.App)
		return &rtmp.RTMPError{
			Code:    rtmp.ErrConnectRejected,
			Message: "unknown application",
		}
	}
	return nil
}

func (h *ingestHandler) OnPublish(s rtmp.Session, info rtmp.PublishInfo) error {
	log.Printf("[ingest] publish id=%s key=%q", s.ID(), info.StreamKey)

	// validate stream key against your auth source.
	if !validKeys[info.StreamKey] {
		log.Printf("[ingest] rejected: invalid key %q", info.StreamKey)
		return &rtmp.RTMPError{
			Code:    rtmp.ErrPublishRejected,
			Message: "invalid stream key",
		}
	}

	//  check for duplicate publish.
	p, ok := h.reg.start(info.StreamKey)
	if !ok {
		log.Printf("[ingest] rejected: key %q already live", info.StreamKey)
		return &rtmp.RTMPError{
			Code:    rtmp.ErrPublishRejected,
			Message: "stream key already in use",
		}
	}

	// Attach pipeline to session so OnPacket can find it.
	s.SetUserData(p)
	log.Printf("[ingest] stream %q is now live", info.StreamKey)
	return nil
}

func (h *ingestHandler) OnMetadata(s rtmp.Session, m rtmp.Metadata) {
	log.Printf("[ingest] metadata id=%s %.0fx%.0f fps=%.1f",
		s.ID(), m.Width, m.Height, m.FrameRate)

	// You might store this for the transmuxer or API.
	// p := s.UserData().(*pipeline)
	// p.setMetadata(m)
}

func (h *ingestHandler) OnPacket(s rtmp.Session, pkt *rtmp.Packet) {
	p := s.UserData().(*pipeline)

	// Cache sequence headers so late-joining viewers get decoder config.
	if pkt.IsSequenceHeader {
		cp := pkt.Clone()
		p.mu.Lock()
		if pkt.Type == rtmp.PacketTypeVideo {
			p.videoSeq = cp
		} else {
			p.audioSeq = cp
		}
		p.mu.Unlock()
	}

	// Clone before sending to channel — payload is only valid during this call.
	cp := pkt.Clone()
	if !p.send(cp) {
		// Pipeline is full. Disconnect the slow publisher.
		log.Printf("[ingest] pipeline full for %s, disconnecting", p.key)
		_ = s.Close()
	}
}

func (h *ingestHandler) OnDisconnect(s rtmp.Session, err error) {
	key := s.StreamKey()
	log.Printf("[ingest] disconnect id=%s key=%q err=%v", s.ID(), key, err)

	if key != "" {
		h.reg.stop(key)
		log.Printf("[ingest] stream %q stopped", key)
	}
}

func main() {
	reg := &registry{ss: make(map[string]*pipeline)}
	h := &ingestHandler{reg: reg}

	srv, err := rtmp.NewServer(rtmp.Config{
		ListenAddr:     ":1935",
		MaxConnections: 100,
	}, h)
	if err != nil {
		log.Fatal(err)
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	go func() {
		sg := make(chan os.Signal, 1)
		signal.Notify(sg, syscall.SIGINT, syscall.SIGTERM)
		<-sg
		log.Println("[ingest] shutting down...")
		reg.stopAll()
		ctx, cancel := context.WithTimeout(context.Background(), 5e9)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	log.Println("[ingest] listening on :1935")
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
