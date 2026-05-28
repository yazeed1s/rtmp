package rtmp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/yazeed1s/rtmp/internal/handshake"
	"github.com/yazeed1s/rtmp/internal/session"
)

type Server struct {
	cfg Config
	h   Handler

	ln net.Listener

	ss sync.Map // map[string]*session.Session
	wg sync.WaitGroup
	mu sync.Mutex
	cc int32
}

func NewServer(cfg Config, h Handler) (*Server, error) {
	if h == nil {
		return nil, fmt.Errorf("rtmp: nil handler")
	}

	cfg = applyDefaults(cfg)
	return &Server{
		cfg: cfg,
		h:   h,
	}, nil
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()

	for {
		cn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		if s.cfg.MaxConnections > 0 {
			if atomic.LoadInt32(&s.cc) >= int32(s.cfg.MaxConnections) {
				s.cfg.Logger.Warn("max connections reached")
				_ = cn.Close()
				continue
			}
		}

		atomic.AddInt32(&s.cc, 1)
		s.wg.Add(1)
		go func(cn net.Conn) {
			defer s.wg.Done()
			defer atomic.AddInt32(&s.cc, -1)
			s.runSession(cn)
		}(cn)
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}

	s.ss.Range(func(_, v any) bool {
		ss, ok := v.(*session.Session)
		if ok {
			_ = ss.Close()
		}
		return true
	})

	dn := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(dn)
	}()

	select {
	case <-dn:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) runSession(cn net.Conn) {
	err := handshake.DoServer(cn, s.cfg.HandshakeTimeout)
	if err != nil {
		s.cfg.Logger.Warn("handshake failed: %v", err)
		_ = cn.Close()
		return
	}

	sc := session.Config{
		ReadTimeout:    s.cfg.ReadTimeout,
		WriteTimeout:   s.cfg.WriteTimeout,
		MaxChunkSize:   s.cfg.MaxChunkSize,
		MaxMessageSize: s.cfg.MaxMessageSize,
		ReadBufSize:    s.cfg.ReadBufSize,
		WriteBufSize:   s.cfg.WriteBufSize,
		Logger:         s.cfg.Logger,
	}
	sh := sessionHandler{h: s.h}
	ss := session.New(cn, sc, sh)
	s.ss.Store(ss.ID(), ss)
	ss.Run()
	s.ss.Delete(ss.ID())
}

func applyDefaults(cfg Config) Config {
	dc := defaultConfig()

	if cfg.ListenAddr == "" {
		cfg.ListenAddr = dc.ListenAddr
	}
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = dc.HandshakeTimeout
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = dc.WriteTimeout
	}
	if cfg.MaxChunkSize == 0 {
		cfg.MaxChunkSize = dc.MaxChunkSize
	}
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = dc.MaxMessageSize
	}
	if cfg.ReadBufSize == 0 {
		cfg.ReadBufSize = dc.ReadBufSize
	}
	if cfg.WriteBufSize == 0 {
		cfg.WriteBufSize = dc.WriteBufSize
	}
	if cfg.Logger == nil {
		cfg.Logger = dc.Logger
	}

	return cfg
}

type sessionHandler struct {
	h Handler
}

func (a sessionHandler) OnConnect(sess *session.Session, info session.ConnectInfo) error {
	var fcc []FourCC
	if len(info.FourCCList) > 0 {
		fcc = make([]FourCC, len(info.FourCCList))
		for i, v := range info.FourCCList {
			fcc[i] = FourCC(v)
		}
	}
	return a.h.OnConnect(sess, ConnectInfo{
		App:            info.App,
		FlashVer:       info.FlashVer,
		SWFURL:         info.SWFURL,
		TCURL:          info.TCURL,
		ObjectEncoding: info.ObjectEncoding,
		EnhancedRTMP:   info.EnhancedRTMP,
		FourCCList:     fcc,
	})
}

func (a sessionHandler) OnPublish(sess *session.Session, info session.PublishInfo) error {
	return a.h.OnPublish(sess, PublishInfo{
		StreamKey: info.StreamKey,
		Type:      info.Type,
	})
}

func (a sessionHandler) OnMetadata(sess *session.Session, meta session.Metadata) {
	a.h.OnMetadata(sess, Metadata{
		VideoCodecID:    meta.VideoCodecID,
		AudioCodecID:    meta.AudioCodecID,
		Width:           meta.Width,
		Height:          meta.Height,
		FrameRate:       meta.FrameRate,
		VideoDataRate:   meta.VideoDataRate,
		AudioDataRate:   meta.AudioDataRate,
		AudioChannels:   meta.AudioChannels,
		AudioSampleRate: meta.AudioSampleRate,
		Duration:        meta.Duration,
		FileSize:        meta.FileSize,
		Extra:           meta.Extra,
	})
}

func (a sessionHandler) OnPacket(sess *session.Session, pkt *session.Packet) {
	a.h.OnPacket(sess, &Packet{
		Type:             PacketType(pkt.Type),
		FourCC:           FourCC(pkt.FourCC),
		VideoPacketType:  VideoPacketType(pkt.VideoPacketType),
		AudioPacketType:  AudioPacketType(pkt.AudioPacketType),
		IsSequenceHeader: pkt.IsSequenceHeader,
		IsKeyframe:       pkt.IsKeyframe,
		IsEnhanced:       pkt.IsEnhanced,
		Timestamp:        pkt.Timestamp,
		CompositionTime:  pkt.CompositionTime,
		StreamID:         pkt.StreamID,
		Payload:          pkt.Payload,
		AudioCodec:       AudioCodec(pkt.AudioCodec),
		VideoCodec:       VideoCodec(pkt.VideoCodec),
	})
}

func (a sessionHandler) OnDisconnect(sess *session.Session, err error) {
	a.h.OnDisconnect(sess, err)
}
