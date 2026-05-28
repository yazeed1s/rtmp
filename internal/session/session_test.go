package session

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/yazeed1s/rtmp/internal/amf0"
	"github.com/yazeed1s/rtmp/internal/chunk"
	"github.com/yazeed1s/rtmp/internal/control"
	"github.com/yazeed1s/rtmp/internal/message"
	"github.com/yazeed1s/rtmp/internal/pool"
)

type recHandler struct {
	mu sync.Mutex

	rejPub bool

	connN int
	pubN  int
	metaN int
	pktN  int

	connInfo ConnectInfo
	pubInfo  PublishInfo
	meta     Metadata
	pkts     []Packet

	discErr error
	discCh  chan struct{}
}

func (h *recHandler) OnConnect(sess *Session, info ConnectInfo) error {
	h.mu.Lock()
	h.connN++
	h.connInfo = info
	h.mu.Unlock()
	return nil
}

func (h *recHandler) OnPublish(sess *Session, info PublishInfo) error {
	h.mu.Lock()
	h.pubN++
	h.pubInfo = info
	rej := h.rejPub
	h.mu.Unlock()
	if rej {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (h *recHandler) OnMetadata(sess *Session, meta Metadata) {
	h.mu.Lock()
	h.metaN++
	h.meta = meta
	h.mu.Unlock()
}

func (h *recHandler) OnPacket(sess *Session, pkt *Packet) {
	cp := *pkt
	cp.Payload = append([]byte(nil), pkt.Payload...)

	h.mu.Lock()
	h.pktN++
	h.pkts = append(h.pkts, cp)
	h.mu.Unlock()
}

func (h *recHandler) OnDisconnect(sess *Session, err error) {
	h.mu.Lock()
	h.discErr = err
	h.mu.Unlock()
	if h.discCh != nil {
		close(h.discCh)
	}
}

func TestSessionConnectPublishMediaFlow(t *testing.T) {
	cl, sv := net.Pipe()
	defer cl.Close()
	defer sv.Close()

	h := &recHandler{discCh: make(chan struct{})}
	s := New(sv, Config{}, h)
	go s.Run()

	// Drain server->client replies so writes never block on net.Pipe.
	dn := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, cl)
		close(dn)
	}()

	w := chunk.NewWriter(bufio.NewWriter(cl))
	w.SetChunkSize(128)

	if err := sendCommand(w, 0, "connect", 1, map[string]any{
		"app":            "live",
		"flashVer":       "FMLE/3.0",
		"swfUrl":         "",
		"tcUrl":          "rtmp://localhost/live",
		"objectEncoding": float64(0),
	}); err != nil {
		t.Fatalf("send connect: %v", err)
	}

	if err := sendCommand(w, 0, "releaseStream", 2, nil, "key1"); err != nil {
		t.Fatalf("send releaseStream: %v", err)
	}
	if err := sendCommand(w, 0, "FCPublish", 3, nil, "key1"); err != nil {
		t.Fatalf("send fcpublish: %v", err)
	}
	if err := sendCommand(w, 0, "createStream", 4, nil); err != nil {
		t.Fatalf("send createStream: %v", err)
	}
	if err := sendCommand(w, 1, "publish", 0, nil, "key1", "live"); err != nil {
		t.Fatalf("send publish: %v", err)
	}

	mp, err := amf0.Encode("@setDataFrame", "onMetaData", map[string]any{
		"width":     float64(1920),
		"height":    float64(1080),
		"framerate": float64(30),
	})
	if err != nil {
		t.Fatalf("encode metadata: %v", err)
	}
	if err := w.WriteMessage(5, 18, 1, 0, mp); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	if err := w.WriteMessage(6, 8, 1, 10, []byte{0xAF, 0x00, 0x12}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := w.WriteMessage(6, 9, 1, 20, []byte{0x17, 0x00, 0x00, 0x00, 0x00, 0x01}); err != nil {
		t.Fatalf("write video h264: %v", err)
	}
	if err := w.WriteMessage(6, 9, 1, 30, []byte{0x90, 'h', 'v', 'c', '1'}); err != nil {
		t.Fatalf("write video enhanced: %v", err)
	}

	_ = cl.Close()

	select {
	case <-h.discCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting disconnect")
	}
	<-dn

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.connN != 1 {
		t.Fatalf("connect count mismatch: got=%d want=1", h.connN)
	}
	if h.pubN != 1 {
		t.Fatalf("publish count mismatch: got=%d want=1", h.pubN)
	}
	if h.metaN != 1 {
		t.Fatalf("metadata count mismatch: got=%d want=1", h.metaN)
	}
	if h.pktN != 3 {
		t.Fatalf("packet count mismatch: got=%d want=3", h.pktN)
	}
	if h.connInfo.App != "live" {
		t.Fatalf("connect app mismatch: got=%q want=live", h.connInfo.App)
	}
	if h.pubInfo.StreamKey != "key1" {
		t.Fatalf("publish stream key mismatch: got=%q want=key1", h.pubInfo.StreamKey)
	}
	if h.meta.Width != 1920 || h.meta.Height != 1080 {
		t.Fatalf("metadata mismatch: got=%+v", h.meta)
	}

	a := h.pkts[0]
	if a.Type != PacketTypeAudio || a.AudioCodec != AudioCodecAAC || !a.IsSequenceHeader || a.FourCC != FourCCAAC || a.AudioPacketType != AudioPacketSequenceStart {
		t.Fatalf("audio packet mismatch: %+v", a)
	}

	v := h.pkts[1]
	if v.Type != PacketTypeVideo || v.VideoCodec != VideoCodecH264 || !v.IsSequenceHeader || !v.IsKeyframe || v.FourCC != FourCCAVC || v.VideoPacketType != VideoPacketSequenceStart {
		t.Fatalf("video packet mismatch: %+v", v)
	}

	ev := h.pkts[2]
	if ev.VideoCodec != VideoCodecEnhanced || ev.FourCC != FourCCHEVC || ev.VideoPacketType != VideoPacketSequenceStart {
		t.Fatalf("enhanced packet mismatch: %+v", ev)
	}

	if h.discErr != nil {
		t.Fatalf("disconnect err mismatch: got=%v want=nil", h.discErr)
	}
}

func TestSessionPublishRejected(t *testing.T) {
	cl, sv := net.Pipe()
	defer cl.Close()
	defer sv.Close()

	h := &recHandler{
		rejPub: true,
		discCh: make(chan struct{}),
	}
	s := New(sv, Config{}, h)
	go s.Run()

	go func() {
		_, _ = io.Copy(io.Discard, cl)
	}()

	w := chunk.NewWriter(bufio.NewWriter(cl))
	if err := sendCommand(w, 0, "connect", 1, map[string]any{
		"app":            "live",
		"tcUrl":          "rtmp://localhost/live",
		"objectEncoding": float64(0),
	}); err != nil {
		t.Fatalf("send connect: %v", err)
	}
	if err := sendCommand(w, 0, "createStream", 2, nil); err != nil {
		t.Fatalf("send createStream: %v", err)
	}
	if err := sendCommand(w, 1, "publish", 0, nil, "bad", "live"); err != nil {
		t.Fatalf("send publish: %v", err)
	}

	select {
	case <-h.discCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting disconnect")
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.discErr == nil {
		t.Fatal("expected disconnect error for rejected publish")
	}
}

func TestSessionPingRequestResponse(t *testing.T) {
	cl, sv := net.Pipe()
	defer cl.Close()
	defer sv.Close()

	h := &recHandler{discCh: make(chan struct{})}
	s := New(sv, Config{}, h)
	go s.Run()

	w := chunk.NewWriter(bufio.NewWriter(cl))
	req := make([]byte, 6)
	binary.BigEndian.PutUint16(req[:2], 6)
	binary.BigEndian.PutUint32(req[2:], 0x11223344)
	if err := w.WriteMessage(2, 4, 0, 0, req); err != nil {
		t.Fatalf("write ping request: %v", err)
	}

	br := chunk.NewReader(bufio.NewReader(cl))
	as := message.NewAssembler(pool.New(), 1024)

	hd, pl, err := br.ReadChunk()
	if err != nil {
		t.Fatalf("read response chunk: %v", err)
	}
	msg, err := as.Feed(hd, pl)
	if err != nil {
		t.Fatalf("assemble response: %v", err)
	}
	if msg == nil {
		t.Fatal("expected complete response message")
	}
	defer msg.Release()

	if msg.TypeID != 4 {
		t.Fatalf("response type mismatch: got=%d want=4", msg.TypeID)
	}
	ev, dt, err := control.ParseUserControl(msg.Payload)
	if err != nil {
		t.Fatalf("parse user control: %v", err)
	}
	if ev != 7 {
		t.Fatalf("event mismatch: got=%d want=7", ev)
	}
	if len(dt) != 4 {
		t.Fatalf("event data len mismatch: got=%d want=4", len(dt))
	}
	if binary.BigEndian.Uint32(dt) != 0x11223344 {
		t.Fatalf("timestamp mismatch: got=0x%08x want=0x11223344", binary.BigEndian.Uint32(dt))
	}

	_ = cl.Close()
	select {
	case <-h.discCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting disconnect")
	}
}

func sendCommand(w *chunk.Writer, sid uint32, nm string, tx float64, as ...any) error {
	pl, err := amf0.EncodeCommand(nm, tx, as...)
	if err != nil {
		return err
	}
	return w.WriteMessage(3, 20, sid, 0, pl)
}
