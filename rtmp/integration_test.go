package rtmp

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/yazeed1s/rtmp/internal/amf0"
	"github.com/yazeed1s/rtmp/internal/chunk"
	"github.com/yazeed1s/rtmp/internal/message"
	"github.com/yazeed1s/rtmp/internal/pool"
)

type recordingHandler struct {
	mu sync.Mutex

	connectN int
	publishN int
	metaN    int
	packetN  int
	discN    int

	connectInfo ConnectInfo
	publishInfo PublishInfo
	meta        Metadata
	packets     []Packet
	discErr     error
	discCh      chan struct{}
	discOnce    sync.Once
}

func (h *recordingHandler) OnConnect(sess Session, info ConnectInfo) error {
	h.mu.Lock()
	h.connectN++
	h.connectInfo = info
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) OnPublish(sess Session, info PublishInfo) error {
	h.mu.Lock()
	h.publishN++
	h.publishInfo = info
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) OnMetadata(sess Session, meta Metadata) {
	h.mu.Lock()
	h.metaN++
	h.meta = meta
	h.mu.Unlock()
}

func (h *recordingHandler) OnPacket(sess Session, pkt *Packet) {
	cp := *pkt
	cp.Payload = append([]byte(nil), pkt.Payload...)

	h.mu.Lock()
	h.packetN++
	h.packets = append(h.packets, cp)
	h.mu.Unlock()
}

func (h *recordingHandler) OnDisconnect(sess Session, err error) {
	h.mu.Lock()
	h.discN++
	h.discErr = err
	h.mu.Unlock()
	h.discOnce.Do(func() { close(h.discCh) })
}

type rtmpClient struct {
	cn net.Conn
	w  *chunk.Writer
	r  *chunk.Reader
	as *message.Assembler
}

func newRTMPClient(addr string) (*rtmpClient, error) {
	cn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if err := doClientHandshake(cn); err != nil {
		_ = cn.Close()
		return nil, err
	}

	cl := &rtmpClient{
		cn: cn,
		w:  chunk.NewWriter(bufio.NewWriter(cn)),
		r:  chunk.NewReader(bufio.NewReader(cn)),
		as: message.NewAssembler(pool.New(), 4*1024*1024),
	}
	// Server default chunk size is 128 until peer changes it.
	cl.r.SetChunkSize(128)
	return cl, nil
}

func (c *rtmpClient) close() {
	_ = c.cn.Close()
}

func (c *rtmpClient) sendCommand(streamID uint32, name string, tx float64, args ...any) error {
	pl, err := amf0.EncodeCommand(name, tx, args...)
	if err != nil {
		return err
	}
	return c.w.WriteMessage(3, 20, streamID, 0, pl)
}

func (c *rtmpClient) sendData(streamID uint32, ts uint32, vals ...any) error {
	pl, err := amf0.Encode(vals...)
	if err != nil {
		return err
	}
	return c.w.WriteMessage(5, 18, streamID, ts, pl)
}

func (c *rtmpClient) sendVideo(ts uint32, p []byte) error {
	return c.w.WriteMessage(6, 9, 1, ts, p)
}

func (c *rtmpClient) sendAudio(ts uint32, p []byte) error {
	return c.w.WriteMessage(6, 8, 1, ts, p)
}

func (c *rtmpClient) waitCommand(name string, to time.Duration) ([]any, error) {
	dl := time.Now().Add(to)
	for time.Now().Before(dl) {
		_ = c.cn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))

		h, p, err := c.r.ReadChunk()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return nil, err
		}

		msg, err := c.as.Feed(h, p)
		if err != nil {
			return nil, err
		}
		if msg == nil {
			continue
		}

		if msg.TypeID != 20 {
			msg.Release()
			continue
		}

		vs, err := amf0.Decode(msg.Payload)
		msg.Release()
		if err != nil || len(vs) == 0 {
			continue
		}

		nm, ok := vs[0].(string)
		if !ok {
			continue
		}
		if nm == name {
			return vs, nil
		}
	}

	return nil, fmt.Errorf("timeout waiting command %q", name)
}

func TestFullPublishFlow(t *testing.T) {
	h := &recordingHandler{discCh: make(chan struct{})}
	s, err := NewServer(Config{
		ListenAddr:       "127.0.0.1:0",
		HandshakeTimeout: time.Second,
	}, h)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.ListenAndServe()
	}()

	addr := waitAddr(t, s, time.Second)
	cl, err := newRTMPClient(addr)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.close()

	if err := cl.sendCommand(0, "connect", 1, map[string]any{
		"app":            "live",
		"flashVer":       "FMLE/3.0",
		"tcUrl":          "rtmp://localhost/live",
		"objectEncoding": float64(0),
	}); err != nil {
		t.Fatalf("send connect: %v", err)
	}
	if _, err := cl.waitCommand("_result", 2*time.Second); err != nil {
		t.Fatalf("wait connect result: %v", err)
	}

	if err := cl.sendCommand(0, "releaseStream", 2, nil, "testkey"); err != nil {
		t.Fatalf("send releaseStream: %v", err)
	}
	if _, err := cl.waitCommand("_result", 2*time.Second); err != nil {
		t.Fatalf("wait releaseStream result: %v", err)
	}

	if err := cl.sendCommand(0, "FCPublish", 3, nil, "testkey"); err != nil {
		t.Fatalf("send fcpublish: %v", err)
	}
	if _, err := cl.waitCommand("onFCPublish", 2*time.Second); err != nil {
		t.Fatalf("wait onFCPublish: %v", err)
	}

	if err := cl.sendCommand(0, "createStream", 4, nil); err != nil {
		t.Fatalf("send createStream: %v", err)
	}
	if _, err := cl.waitCommand("_result", 2*time.Second); err != nil {
		t.Fatalf("wait createStream result: %v", err)
	}

	if err := cl.sendCommand(1, "publish", 0, nil, "testkey", "live"); err != nil {
		t.Fatalf("send publish: %v", err)
	}
	if _, err := cl.waitCommand("onStatus", 2*time.Second); err != nil {
		t.Fatalf("wait publish onStatus: %v", err)
	}

	if err := cl.sendData(1, 0, "@setDataFrame", "onMetaData", map[string]any{
		"width":     float64(1920),
		"height":    float64(1080),
		"framerate": float64(30),
	}); err != nil {
		t.Fatalf("send metadata: %v", err)
	}

	if err := cl.sendVideo(10, []byte{0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64, 0x00}); err != nil {
		t.Fatalf("send video 1: %v", err)
	}
	if err := cl.sendVideo(20, []byte{0x17, 0x01, 0x00, 0x00, 0x00, 0x65, 0x88}); err != nil {
		t.Fatalf("send video 2: %v", err)
	}
	if err := cl.sendVideo(30, []byte{0x27, 0x01, 0x00, 0x00, 0x00, 0x41, 0x9A}); err != nil {
		t.Fatalf("send video 3: %v", err)
	}
	if err := cl.sendVideo(40, []byte{0x27, 0x01, 0x00, 0x00, 0x00, 0x41, 0x9B}); err != nil {
		t.Fatalf("send video 4: %v", err)
	}
	if err := cl.sendVideo(50, []byte{0x27, 0x01, 0x00, 0x00, 0x00, 0x41, 0x9C}); err != nil {
		t.Fatalf("send video 5: %v", err)
	}

	if err := cl.sendAudio(15, []byte{0xAF, 0x00, 0x12, 0x10}); err != nil {
		t.Fatalf("send audio 1: %v", err)
	}
	if err := cl.sendAudio(25, []byte{0xAF, 0x01, 0xAA}); err != nil {
		t.Fatalf("send audio 2: %v", err)
	}
	if err := cl.sendAudio(35, []byte{0xAF, 0x01, 0xAB}); err != nil {
		t.Fatalf("send audio 3: %v", err)
	}
	if err := cl.sendAudio(45, []byte{0xAF, 0x01, 0xAC}); err != nil {
		t.Fatalf("send audio 4: %v", err)
	}
	if err := cl.sendAudio(55, []byte{0xAF, 0x01, 0xAD}); err != nil {
		t.Fatalf("send audio 5: %v", err)
	}

	// Let server consume all media before socket close.
	time.Sleep(150 * time.Millisecond)
	cl.close()

	select {
	case <-h.discCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting disconnect")
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.connectN != 1 {
		t.Fatalf("connect count mismatch: got=%d want=1", h.connectN)
	}
	if h.publishN != 1 {
		t.Fatalf("publish count mismatch: got=%d want=1", h.publishN)
	}
	if h.metaN != 1 {
		t.Fatalf("metadata count mismatch: got=%d want=1", h.metaN)
	}
	if h.packetN != 10 {
		t.Fatalf("packet count mismatch: got=%d want=10", h.packetN)
	}
	if h.connectInfo.App != "live" {
		t.Fatalf("connect app mismatch: got=%q want=live", h.connectInfo.App)
	}
	if h.publishInfo.StreamKey != "testkey" {
		t.Fatalf("stream key mismatch: got=%q want=testkey", h.publishInfo.StreamKey)
	}
	if h.meta.Width != 1920 || h.meta.Height != 1080 {
		t.Fatalf("metadata mismatch: got width=%v height=%v", h.meta.Width, h.meta.Height)
	}

	var fv *Packet
	var fa *Packet
	for i := range h.packets {
		p := &h.packets[i]
		if p.Type == PacketTypeVideo && fv == nil {
			fv = p
		}
		if p.Type == PacketTypeAudio && fa == nil {
			fa = p
		}
	}

	if fv == nil || fa == nil {
		t.Fatalf("missing first audio/video packets: video=%v audio=%v", fv != nil, fa != nil)
	}
	if !fv.IsSequenceHeader || !fv.IsKeyframe || fv.VideoCodec != VideoCodecH264 {
		t.Fatalf("first video packet mismatch: %+v", *fv)
	}
	if !fa.IsSequenceHeader || fa.AudioCodec != AudioCodecAAC {
		t.Fatalf("first audio packet mismatch: %+v", *fa)
	}
	if h.discErr != nil {
		t.Fatalf("disconnect error mismatch: got=%v want=nil", h.discErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("listen and serve error: %v", err)
	}
}

func TestGracefulShutdown(t *testing.T) {
	h := &recordingHandler{discCh: make(chan struct{})}
	s, err := NewServer(Config{
		ListenAddr:       "127.0.0.1:0",
		HandshakeTimeout: time.Second,
		ReadTimeout:      2 * time.Second,
		WriteTimeout:     2 * time.Second,
	}, h)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.ListenAndServe()
	}()

	addr := waitAddr(t, s, time.Second)
	cl, err := newRTMPClient(addr)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.close()

	if err := cl.sendCommand(0, "connect", 1, map[string]any{
		"app":            "live",
		"flashVer":       "FMLE/3.0",
		"tcUrl":          "rtmp://localhost/live",
		"objectEncoding": float64(0),
	}); err != nil {
		t.Fatalf("send connect: %v", err)
	}
	if _, err := cl.waitCommand("_result", 2*time.Second); err != nil {
		t.Fatalf("wait connect result: %v", err)
	}
	if err := cl.sendCommand(0, "createStream", 2, nil); err != nil {
		t.Fatalf("send createStream: %v", err)
	}
	if _, err := cl.waitCommand("_result", 2*time.Second); err != nil {
		t.Fatalf("wait createStream result: %v", err)
	}
	if err := cl.sendCommand(1, "publish", 0, nil, "testkey", "live"); err != nil {
		t.Fatalf("send publish: %v", err)
	}
	if _, err := cl.waitCommand("onStatus", 2*time.Second); err != nil {
		t.Fatalf("wait publish onStatus: %v", err)
	}

	var wg sync.WaitGroup
	st := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()

		ts := uint32(0)
		for {
			select {
			case <-st:
				return
			default:
			}

			_ = cl.cn.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
			err := cl.sendAudio(ts, []byte{0xAF, 0x01, 0x11, 0x22, 0x33})
			if err != nil {
				return
			}
			ts += 23
			time.Sleep(5 * time.Millisecond)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	close(st)
	wg.Wait()

	select {
	case <-h.discCh:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting disconnect callback")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("listen and serve error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listen and serve did not return after shutdown")
	}
}
