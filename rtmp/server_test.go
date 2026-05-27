package rtmp

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

type testHandler struct{}

func (h testHandler) OnConnect(sess Session, info ConnectInfo) error { return nil }
func (h testHandler) OnPublish(sess Session, info PublishInfo) error { return nil }
func (h testHandler) OnMetadata(sess Session, meta Metadata)         {}
func (h testHandler) OnPacket(sess Session, pkt *Packet)             {}
func (h testHandler) OnDisconnect(sess Session, err error)           {}

func TestServerHandshakeAndShutdown(t *testing.T) {
	s, err := NewServer(Config{
		ListenAddr:       "127.0.0.1:0",
		HandshakeTimeout: time.Second,
	}, testHandler{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.ListenAndServe()
	}()

	addr := waitAddr(t, s, time.Second)
	cn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	if err := doClientHandshake(cn); err != nil {
		_ = cn.Close()
		t.Fatalf("client handshake: %v", err)
	}
	_ = cn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("listen and serve error: %v", err)
	}
}

func waitAddr(t *testing.T, s *Server, to time.Duration) string {
	t.Helper()
	dl := time.Now().Add(to)
	for time.Now().Before(dl) {
		s.mu.Lock()
		ln := s.ln
		s.mu.Unlock()
		if ln != nil {
			return ln.Addr().String()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("listener not ready")
	return ""
}

func doClientHandshake(cn net.Conn) error {
	var c1 [1536]byte
	binary.BigEndian.PutUint32(c1[0:4], 0x01020304)
	c1[4], c1[5], c1[6], c1[7] = 0, 0, 0, 0
	for i := 8; i < len(c1); i++ {
		c1[i] = byte(i % 251)
	}

	c0c1 := append([]byte{0x03}, c1[:]...)
	if _, err := cn.Write(c0c1); err != nil {
		return err
	}

	var srv [1 + 1536 + 1536]byte
	if _, err := io.ReadFull(cn, srv[:]); err != nil {
		return err
	}
	if srv[0] != 0x03 {
		return &RTMPError{Code: ErrHandshakeFailed, Message: "bad s0 version"}
	}

	s1 := srv[1 : 1+1536]
	var c2 [1536]byte
	copy(c2[0:4], s1[0:4])
	binary.BigEndian.PutUint32(c2[4:8], 0)
	copy(c2[8:], s1[8:])

	_, err := cn.Write(c2[:])
	return err
}
