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

// FourCC is a four-character codec identifier per the E-RTMP spec.
// Encoded big-endian: 'a','v','c','1' = 0x61766331.
type FourCC uint32

const (
	FourCCNone FourCC = 0

	// Video codecs.
	FourCCAVC  FourCC = 'a'<<24 | 'v'<<16 | 'c'<<8 | '1'
	FourCCHEVC FourCC = 'h'<<24 | 'v'<<16 | 'c'<<8 | '1'
	FourCCAV1  FourCC = 'a'<<24 | 'v'<<16 | '0'<<8 | '1'
	FourCCVP9  FourCC = 'v'<<24 | 'p'<<16 | '0'<<8 | '9'
	FourCCVP8  FourCC = 'v'<<24 | 'p'<<16 | '0'<<8 | '8'
	FourCCVVC  FourCC = 'v'<<24 | 'v'<<16 | 'c'<<8 | '1'

	// Audio codecs.
	FourCCAAC  FourCC = 'm'<<24 | 'p'<<16 | '4'<<8 | 'a'
	FourCCOpus FourCC = 'O'<<24 | 'p'<<16 | 'u'<<8 | 's'
	FourCCMP3  FourCC = '.'<<24 | 'm'<<16 | 'p'<<8 | '3'
	FourCCFLAC FourCC = 'f'<<24 | 'L'<<16 | 'a'<<8 | 'C'
	FourCCAC3  FourCC = 'a'<<24 | 'c'<<16 | '-'<<8 | '3'
	FourCCEAC3 FourCC = 'e'<<24 | 'c'<<16 | '-'<<8 | '3'
)

// String returns the four ASCII characters of the FourCC.
func (f FourCC) String() string {
	if f == FourCCNone {
		return "none"
	}
	b := [4]byte{byte(f >> 24), byte(f >> 16), byte(f >> 8), byte(f)}
	return string(b[:])
}

// VideoPacketType identifies the kind of enhanced video packet.
// Values match the E-RTMP spec ExVideoTagHeader enum.
type VideoPacketType uint8

const (
	VideoPacketSequenceStart   VideoPacketType = 0
	VideoPacketCodedFrames     VideoPacketType = 1
	VideoPacketSequenceEnd     VideoPacketType = 2
	VideoPacketCodedFramesX    VideoPacketType = 3 // composition time implied zero
	VideoPacketMetadata        VideoPacketType = 4
	VideoPacketMPEG2TSSeqStart VideoPacketType = 5
	VideoPacketMultitrack      VideoPacketType = 6
	VideoPacketModEx           VideoPacketType = 7
)

// AudioPacketType identifies the kind of enhanced audio packet.
// Values match the E-RTMP spec ExAudioTagHeader enum.
type AudioPacketType uint8

const (
	AudioPacketSequenceStart      AudioPacketType = 0
	AudioPacketCodedFrames        AudioPacketType = 1
	AudioPacketSequenceEnd        AudioPacketType = 2
	AudioPacketMultichannelConfig AudioPacketType = 4
	AudioPacketMultitrack         AudioPacketType = 5
	AudioPacketModEx              AudioPacketType = 7
)

// Deprecated: use FourCC instead for both classic and enhanced tags.
type VideoCodec uint8

const (
	VideoCodecH264     VideoCodec = 0x07
	VideoCodecEnhanced VideoCodec = 0xFF
)

// Deprecated: use FourCC instead for both classic and enhanced tags.
type AudioCodec uint8

const (
	AudioCodecAAC AudioCodec = 0x0A
	AudioCodecMP3 AudioCodec = 0x02
)

// Packet is the public media packet delivered via Handler.OnPacket.
// FourCC is always set for both classic and enhanced tags.
// VideoPacketType / AudioPacketType are only valid for enhanced tags.
type Packet struct {
	Type             PacketType
	FourCC           FourCC
	VideoPacketType  VideoPacketType // valid when Type == Video && IsEnhanced
	AudioPacketType  AudioPacketType // valid when Type == Audio && IsEnhanced
	IsSequenceHeader bool
	IsKeyframe       bool
	IsEnhanced       bool // true when parsed from an enhanced tag
	Timestamp        uint32
	CompositionTime  int32
	StreamID         uint32
	Payload          []byte

	// Deprecated: use FourCC.
	AudioCodec AudioCodec
	// Deprecated: use FourCC.
	VideoCodec VideoCodec
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
	EnhancedRTMP   bool     // true if publisher advertised E-RTMP support
	FourCCList     []FourCC // codecs advertised in connect fourCcList
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
