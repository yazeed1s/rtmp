package rtmp

import (
	"net"
	"time"
)

type PacketType uint8

const (
	PacketTypeAudio PacketType = 0x08
	PacketTypeVideo PacketType = 0x09
)

type VideoCodec uint8

const (
	VideoCodecH264     VideoCodec = 0x07
	VideoCodecEnhanced VideoCodec = 0xFF
)

type AudioCodec uint8

const (
	AudioCodecAAC AudioCodec = 0x0A
	AudioCodecMP3 AudioCodec = 0x02
)

type Packet struct {
	Type             PacketType
	AudioCodec       AudioCodec
	VideoCodec       VideoCodec
	IsSequenceHeader bool
	IsKeyframe       bool
	Timestamp        uint32
	CompositionTime  int32
	StreamID         uint32
	Payload          []byte
}

func (p *Packet) Clone() *Packet {
	if p == nil {
		return nil
	}

	cp := *p
	if p.Payload == nil {
		return &cp
	}

	cp.Payload = make([]byte, len(p.Payload))
	copy(cp.Payload, p.Payload)
	return &cp
}

type ConnectInfo struct {
	App            string
	FlashVer       string
	SWFURL         string
	TCURL          string
	ObjectEncoding int
}

type PublishInfo struct {
	StreamKey string
	Type      string
}

type Metadata struct {
	VideoCodecID    float64
	AudioCodecID    float64
	Width           float64
	Height          float64
	FrameRate       float64
	VideoDataRate   float64
	AudioDataRate   float64
	AudioChannels   float64
	AudioSampleRate float64
	Duration        float64
	FileSize        float64
	Extra           map[string]any
}

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type noopLogger struct{}

func (n noopLogger) Debug(msg string, args ...any) {}
func (n noopLogger) Info(msg string, args ...any)  {}
func (n noopLogger) Warn(msg string, args ...any)  {}
func (n noopLogger) Error(msg string, args ...any) {}

type Config struct {
	ListenAddr       string
	MaxConnections   int
	HandshakeTimeout time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	MaxChunkSize     int
	MaxMessageSize   int
	ReadBufSize      int
	WriteBufSize     int
	Logger           Logger
}

func defaultConfig() Config {
	return Config{
		ListenAddr:       ":1935",
		MaxConnections:   0,
		HandshakeTimeout: 10 * time.Second,
		ReadTimeout:      0,
		WriteTimeout:     5 * time.Second,
		MaxChunkSize:     65536,
		MaxMessageSize:   4 * 1024 * 1024,
		ReadBufSize:      4096,
		WriteBufSize:     4096,
		Logger:           noopLogger{},
	}
}

type Handler interface {
	OnConnect(sess Session, info ConnectInfo) error
	OnPublish(sess Session, info PublishInfo) error
	OnMetadata(sess Session, meta Metadata)
	OnPacket(sess Session, pkt *Packet)
	OnDisconnect(sess Session, err error)
}

type Session interface {
	ID() string
	RemoteAddr() net.Addr
	AppName() string
	StreamKey() string
	SetUserData(v any)
	UserData() any
	Close() error
}

type ErrorCode uint16

const (
	ErrHandshakeFailed ErrorCode = iota + 1
	ErrBadChunkHeader
	ErrMessageTooLarge
	ErrBadAMF
	ErrProtocolViolation
	ErrReadTimeout
	ErrWriteTimeout
	ErrPublishRejected
	ErrConnectRejected
	ErrPeerDisconnected
)

type RTMPError struct {
	Code    ErrorCode
	Message string
	Wrapped error
}

func (e *RTMPError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" && e.Wrapped == nil {
		return "rtmp error"
	}
	if e.Wrapped == nil {
		return e.Message
	}
	if e.Message == "" {
		return e.Wrapped.Error()
	}
	return e.Message + ": " + e.Wrapped.Error()
}

func (e *RTMPError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Wrapped
}
