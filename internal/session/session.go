package session

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/yazeed1s/rtmp/internal/amf0"
	"github.com/yazeed1s/rtmp/internal/bytecounter"
	"github.com/yazeed1s/rtmp/internal/chunk"
	"github.com/yazeed1s/rtmp/internal/control"
	"github.com/yazeed1s/rtmp/internal/message"
	"github.com/yazeed1s/rtmp/internal/pool"
)

type PacketType uint8

const (
	PacketTypeAudio PacketType = 0x08
	PacketTypeVideo PacketType = 0x09
)

type FourCC uint32

const (
	FourCCNone FourCC = 0
	FourCCAVC  FourCC = 'a'<<24 | 'v'<<16 | 'c'<<8 | '1'
	FourCCHEVC FourCC = 'h'<<24 | 'v'<<16 | 'c'<<8 | '1'
	FourCCAV1  FourCC = 'a'<<24 | 'v'<<16 | '0'<<8 | '1'
	FourCCVP9  FourCC = 'v'<<24 | 'p'<<16 | '0'<<8 | '9'
	FourCCVP8  FourCC = 'v'<<24 | 'p'<<16 | '0'<<8 | '8'
	FourCCVVC  FourCC = 'v'<<24 | 'v'<<16 | 'c'<<8 | '1'
	FourCCAAC  FourCC = 'm'<<24 | 'p'<<16 | '4'<<8 | 'a'
	FourCCOpus FourCC = 'O'<<24 | 'p'<<16 | 'u'<<8 | 's'
	FourCCMP3  FourCC = '.'<<24 | 'm'<<16 | 'p'<<8 | '3'
	FourCCFLAC FourCC = 'f'<<24 | 'L'<<16 | 'a'<<8 | 'C'
	FourCCAC3  FourCC = 'a'<<24 | 'c'<<16 | '-'<<8 | '3'
	FourCCEAC3 FourCC = 'e'<<24 | 'c'<<16 | '-'<<8 | '3'
)

type VideoPacketType uint8

const (
	VideoPacketSequenceStart   VideoPacketType = 0
	VideoPacketCodedFrames     VideoPacketType = 1
	VideoPacketSequenceEnd     VideoPacketType = 2
	VideoPacketCodedFramesX    VideoPacketType = 3
	VideoPacketMetadata        VideoPacketType = 4
	VideoPacketMPEG2TSSeqStart VideoPacketType = 5
	VideoPacketMultitrack      VideoPacketType = 6
	VideoPacketModEx           VideoPacketType = 7
)

type AudioPacketType uint8

const (
	AudioPacketSequenceStart      AudioPacketType = 0
	AudioPacketCodedFrames        AudioPacketType = 1
	AudioPacketSequenceEnd        AudioPacketType = 2
	AudioPacketMultichannelConfig AudioPacketType = 4
	AudioPacketMultitrack         AudioPacketType = 5
	AudioPacketModEx              AudioPacketType = 7
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
	Type            PacketType
	FourCC          FourCC
	VideoPacketType VideoPacketType
	AudioPacketType AudioPacketType
	IsSequenceHeader bool
	IsKeyframe       bool
	IsEnhanced       bool
	Timestamp        uint32
	CompositionTime  int32
	StreamID         uint32
	Payload          []byte
	AudioCodec       AudioCodec
	VideoCodec       VideoCodec
}

type ConnectInfo struct {
	App            string
	FlashVer       string
	SWFURL         string
	TCURL          string
	ObjectEncoding int
	EnhancedRTMP   bool
	FourCCList     []FourCC
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

func (l noopLogger) Debug(msg string, args ...any) {}
func (l noopLogger) Info(msg string, args ...any)  {}
func (l noopLogger) Warn(msg string, args ...any)  {}
func (l noopLogger) Error(msg string, args ...any) {}

type Config struct {
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxChunkSize   int
	MaxMessageSize int
	ReadBufSize    int
	WriteBufSize   int
	Logger         Logger
}

type Handler interface {
	OnConnect(sess *Session, info ConnectInfo) error
	OnPublish(sess *Session, info PublishInfo) error
	OnMetadata(sess *Session, meta Metadata)
	OnPacket(sess *Session, pkt *Packet)
	OnDisconnect(sess *Session, err error)
}

type phase uint8

const (
	phaseHandshaked phase = iota
	phaseConnecting
	phasePublishing
	phaseClosed
)

var errPeerClosed = errors.New("peer closed stream")

type Session struct {
	id string
	cn net.Conn
	cf Config
	h  Handler

	lg Logger
	bc *bytecounter.ReadWriter
	cr *chunk.Reader
	cw *chunk.Writer
	as *message.Assembler

	ph phase
	ap string
	sk string
	ud any

	las uint32
	wsz uint32

	mu sync.RWMutex
	wo sync.Mutex
	oc sync.Once
	ch chan struct{}
}

func New(cn net.Conn, cf Config, h Handler) *Session {
	if cf.ReadBufSize <= 0 {
		cf.ReadBufSize = 4096
	}
	if cf.WriteBufSize <= 0 {
		cf.WriteBufSize = 4096
	}
	if cf.MaxMessageSize <= 0 {
		cf.MaxMessageSize = 4 * 1024 * 1024
	}
	if cf.MaxChunkSize <= 0 {
		cf.MaxChunkSize = 65536
	}
	if cf.Logger == nil {
		cf.Logger = noopLogger{}
	}

	bc := bytecounter.NewReadWriter(cn)
	br := bufio.NewReaderSize(bc.Reader, cf.ReadBufSize)
	bw := bufio.NewWriterSize(bc.Writer, cf.WriteBufSize)

	return &Session{
		id:  mkID(),
		cn:  cn,
		cf:  cf,
		h:   h,
		lg:  cf.Logger,
		bc:  bc,
		cr:  chunk.NewReader(br),
		cw:  chunk.NewWriter(bw),
		as:  message.NewAssembler(pool.New(), cf.MaxMessageSize),
		ph:  phaseHandshaked,
		wsz: 2500000,
		ch:  make(chan struct{}),
	}
}

func (s *Session) Run() {
	s.inf("run start remote=%s", s.cn.RemoteAddr())
	err := s.runLoop()
	rawErr := err
	if errors.Is(err, io.EOF) || errors.Is(err, errPeerClosed) || isClosedNetErr(err) {
		s.dbg("run loop closed by peer: %v", err)
		err = nil
	}
	if err != nil {
		s.err("run loop failed: %v", err)
	}

	s.mu.Lock()
	s.ph = phaseClosed
	s.mu.Unlock()
	if rawErr == nil {
		s.inf("run stop clean")
	} else if err == nil {
		s.inf("run stop normal close")
	}
	s.h.OnDisconnect(s, err)
}

// runLoop is the main event loop, everything runs in this one single goroutine.
// No channels or mutexes between reader and writer threads.
// If the handler (OnPacket) takes too much time, this loop blocks,
// which gives us free backpressure because the TCP receive buffer fills up
// and tells the sender to slow down naturally.
func (s *Session) runLoop() error {
	rd := s.cr.ReadChunkBody

	for {
		select {
		case <-s.ch:
			return nil
		default:
		}

		if s.cf.ReadTimeout > 0 {
			_ = s.cn.SetReadDeadline(time.Now().Add(s.cf.ReadTimeout))
		}

		h, n, err := s.cr.ReadChunkHeader()
		if err != nil {
			return err
		}
		s.dbg("chunk read csid=0x%X type=0x%02X stream=%d ts=%d msg_len=%d chunk_len=%d new=%t",
			h.CSID, h.MessageTypeID, h.MessageStreamID, h.Timestamp, h.MessageLength, n, h.IsNewMessage)

		cur := uint32(s.bc.Reader.Count())
		// RTMP peer expects ack when bytes pass acknowledgement window.
		if cur-s.las >= s.wsz {
			s.dbg("send ack bytes=%d last=%d window=%d", cur, s.las, s.wsz)
			if err := s.sendAck(cur); err != nil {
				return err
			}
			s.las = cur
		}

		msg, err := s.as.FeedRead(h, n, rd)
		if err != nil {
			return err
		}
		if msg == nil {
			continue
		}
		s.dbg("message assembled type=0x%02X stream=%d ts=%d payload_len=%d",
			msg.TypeID, msg.StreamID, msg.Timestamp, len(msg.Payload))

		err = s.handleMessage(msg)
		msg.Release()
		if err != nil {
			return err
		}
	}
}

func (s *Session) handleMessage(msg *message.RawMessage) error {
	switch msg.TypeID {
	case 1:
		sz, err := control.ParseSetChunkSize(msg.Payload)
		if err != nil {
			return err
		}
		if s.cf.MaxChunkSize > 0 && sz > uint32(s.cf.MaxChunkSize) {
			s.lg.Warn("peer set too large chunk size: %d", sz)
			sz = uint32(s.cf.MaxChunkSize)
		}
		s.cr.SetChunkSize(sz)
		s.dbg("set chunk size=%d", sz)
		return nil

	case 2:
		csid, err := control.ParseAbortMessage(msg.Payload)
		if err != nil {
			return nil
		}
		s.as.Discard(csid)
		s.dbg("abort message csid=0x%X", csid)
		return nil

	case 3:
		seq, err := control.ParseAcknowledgement(msg.Payload)
		if err == nil {
			s.dbg("peer ack sequence=%d", seq)
		}
		return nil

	case 4:
		return s.handleUserControl(msg.Payload)

	case 5:
		sz, err := control.ParseWindowAckSize(msg.Payload)
		if err == nil && sz > 0 {
			old := s.wsz
			s.wsz = sz
			s.dbg("peer window ack size=%d old=%d", sz, old)
		}
		return nil

	case 6:
		sz, lt, err := control.ParseSetPeerBandwidth(msg.Payload)
		if err == nil {
			s.dbg("peer bandwidth size=%d limit_type=%d", sz, lt)
		}
		return nil

	case 18:
		s.dbg("route data message")
		return s.handleDataMessage(msg)

	case 20:
		s.dbg("route command message")
		return s.handleCommandMessage(msg)

	case 17:
		// AMF3 command message is not supported in v1.
		// We close session cleanly instead of trying to parse unknown payload.
		return fmt.Errorf("amf3 command not supported")

	case 8:
		s.dbg("route audio message")
		return s.handleAudio(msg)

	case 9:
		s.dbg("route video message")
		return s.handleVideo(msg)

	default:
		s.dbg("ignore message type=0x%02X", msg.TypeID)
		return nil
	}
}

func (s *Session) handleUserControl(p []byte) error {
	ev, dt, err := control.ParseUserControl(p)
	if err != nil {
		return nil
	}
	s.dbg("user control event=0x%X data_len=%d", ev, len(dt))

	switch ev {
	case 6: // ping request
		if len(dt) < 4 {
			return nil
		}
		ts := binary.BigEndian.Uint32(dt[:4])
		s.dbg("ping request ts=%d", ts)
		return s.writeControl(4, control.BuildPingResponse(ts))
	default:
		return nil
	}
}

func (s *Session) handleCommandMessage(msg *message.RawMessage) error {
	vs, err := amf0.Decode(msg.Payload)
	if err != nil {
		if s.ph == phaseHandshaked || s.ph == phaseConnecting {
			s.wrn("command decode failed in phase=%s: %v", s.phaseName(), err)
			return err
		}
		s.dbg("skip bad command payload in phase=%s: %v", s.phaseName(), err)
		return nil
	}
	if len(vs) < 2 {
		return nil
	}

	nm, ok := vs[0].(string)
	if !ok {
		return nil
	}
	tx, _ := asF64(vs[1])
	s.dbg("command name=%q tx=%.0f args=%d phase=%s", nm, tx, len(vs), s.phaseName())

	switch nm {
	case "connect":
		return s.handleConnect(tx, vs)
	case "releaseStream":
		return s.replyResult(tx, nil, amf0.Undefined)
	case "FCPublish":
		return s.replyFCPublish(vs)
	case "createStream":
		return s.replyResult(tx, nil, float64(1))
	case "publish":
		return s.handlePublish(vs)
	case "deleteStream", "FCUnpublish", "closeStream":
		return errPeerClosed
	case "call":
		if tx > 0 {
			return s.replyResult(tx, nil, nil)
		}
		return nil
	default:
		s.dbg("command fallback name=%q tx=%.0f", nm, tx)
		if tx > 0 {
			return s.replyResult(tx, nil, nil)
		}
		return nil
	}
}

func (s *Session) handleConnect(tx float64, vs []any) error {
	if s.ph != phaseHandshaked {
		return fmt.Errorf("protocol: connect in invalid phase")
	}
	if len(vs) < 3 {
		return fmt.Errorf("protocol: invalid connect payload")
	}

	obj, ok := vs[2].(map[string]any)
	if !ok {
		return fmt.Errorf("protocol: invalid connect object")
	}

	info := ConnectInfo{
		App:            asStr(obj["app"]),
		FlashVer:       asStr(obj["flashVer"]),
		SWFURL:         asStr(obj["swfUrl"]),
		TCURL:          asStr(obj["tcUrl"]),
		ObjectEncoding: int(asNum(obj["objectEncoding"])),
	}

	// E-RTMP: parse fourCcList from connect object.
	// OBS 30+ and ffmpeg send this to advertise codec support.
	if raw, ok := obj["fourCcList"]; ok {
		if arr, ok := raw.([]any); ok && len(arr) > 0 {
			info.EnhancedRTMP = true
			for _, v := range arr {
				str, ok := v.(string)
				if !ok || len(str) != 4 {
					continue
				}
				fc := FourCC(str[0])<<24 | FourCC(str[1])<<16 | FourCC(str[2])<<8 | FourCC(str[3])
				info.FourCCList = append(info.FourCCList, fc)
			}
		}
	}
	if info.TCURL == "" {
		info.TCURL = asStr(obj["tcurl"])
	}
	s.inf("connect app=%q tcurl=%q flash=%q object_encoding=%d",
		info.App, info.TCURL, info.FlashVer, info.ObjectEncoding)

	if err := s.h.OnConnect(s, info); err != nil {
		s.wrn("connect rejected by handler: %v", err)
		_ = s.replyConnectError(tx, err)
		return err
	}

	s.mu.Lock()
	s.ap = info.App
	s.ph = phaseConnecting
	s.mu.Unlock()
	s.inf("phase -> %s", s.phaseName())

	if err := s.writeControl(5, control.BuildWindowAckSize(2500000)); err != nil {
		return err
	}
	if err := s.writeControl(6, control.BuildSetPeerBandwidth(2500000, 2)); err != nil {
		return err
	}
	if err := s.writeControl(4, control.BuildUserControlStreamBegin(0)); err != nil {
		return err
	}

	pr := map[string]any{
		"fmsVer":       "FMS/3,0,1,123",
		"capabilities": float64(31),
	}
	inf := map[string]any{
		"level":          "status",
		"code":           "NetConnection.Connect.Success",
		"description":    "Connection succeeded.",
		"objectEncoding": float64(0),
	}
	return s.replyResult(tx, pr, inf)
}

func (s *Session) handlePublish(vs []any) error {
	if s.ph != phaseConnecting {
		return fmt.Errorf("protocol: publish in invalid phase")
	}

	sk := ""
	pt := "live"
	if len(vs) >= 4 {
		sk = asStr(vs[3])
	}
	if len(vs) >= 5 {
		if t := asStr(vs[4]); t != "" {
			pt = t
		}
	}

	info := PublishInfo{
		StreamKey: sk,
		Type:      pt,
	}
	s.inf("publish stream_key=%q type=%q", info.StreamKey, info.Type)
	if err := s.h.OnPublish(s, info); err != nil {
		s.wrn("publish rejected by handler: %v", err)
		_ = s.replyPublishBadName()
		return err
	}

	s.mu.Lock()
	s.sk = sk
	s.ph = phasePublishing
	s.mu.Unlock()
	s.inf("phase -> %s", s.phaseName())

	if err := s.writeControl(4, control.BuildUserControlStreamBegin(1)); err != nil {
		return err
	}

	inf := map[string]any{
		"level":       "status",
		"code":        "NetStream.Publish.Start",
		"description": "Stream is now published.",
	}
	pl, err := amf0.EncodeCommand("onStatus", 0, nil, inf)
	if err != nil {
		return err
	}
	return s.writeCommandOnStream(1, pl)
}

func (s *Session) handleDataMessage(msg *message.RawMessage) error {
	vs, err := amf0.Decode(msg.Payload)
	if err != nil {
		s.dbg("data message decode failed: %v", err)
		return nil
	}

	var md map[string]any
	if len(vs) >= 3 && asStr(vs[0]) == "@setDataFrame" {
		m, ok := vs[2].(map[string]any)
		if ok {
			md = m
		}
	} else if len(vs) >= 2 && asStr(vs[0]) == "onMetaData" {
		m, ok := vs[1].(map[string]any)
		if ok {
			md = m
		}
	}

	if md == nil {
		s.dbg("data message without metadata frame")
		return nil
	}

	meta := toMeta(md)
	s.inf("metadata width=%.0f height=%.0f vcodec=%.0f acodec=%.0f fps=%.3f extra=%d",
		meta.Width, meta.Height, meta.VideoCodecID, meta.AudioCodecID, meta.FrameRate, len(meta.Extra))
	s.h.OnMetadata(s, meta)
	return nil
}

func (s *Session) handleAudio(msg *message.RawMessage) error {
	if s.ph != phasePublishing {
		return nil
	}
	if len(msg.Payload) < 1 {
		return nil
	}

	c := AudioCodec(msg.Payload[0] >> 4)
	if c == 9 {
		pk, err := parseEnhancedAudio(msg.Payload)
		if err != nil {
			return err
		}
		pk.Timestamp = msg.Timestamp
		pk.StreamID = msg.StreamID
		pk.AudioCodec = 9

		s.dbg("audio packet enhanced fourcc=%s ts=%d size=%d", pk.FourCC, msg.Timestamp, len(msg.Payload))
		s.h.OnPacket(s, &pk)
		return nil
	}

	sh := false
	if c == AudioCodecAAC && len(msg.Payload) >= 2 {
		sh = msg.Payload[1] == 0
	}

	fcc := FourCCNone
	apt := AudioPacketCodedFrames
	if c == AudioCodecAAC {
		fcc = FourCCAAC
		if sh {
			apt = AudioPacketSequenceStart
		}
	} else if c == AudioCodecMP3 {
		fcc = FourCCMP3
	}

	pk := &Packet{
		Type:             PacketTypeAudio,
		FourCC:           fcc,
		AudioPacketType:  apt,
		AudioCodec:       c,
		IsSequenceHeader: sh,
		Timestamp:        msg.Timestamp,
		StreamID:         msg.StreamID,
		Payload:          msg.Payload,
	}
	s.dbg("audio packet codec=0x%X seq_header=%t ts=%d size=%d", c, sh, msg.Timestamp, len(msg.Payload))
	s.h.OnPacket(s, pk)
	return nil
}

func (s *Session) handleVideo(msg *message.RawMessage) error {
	if s.ph != phasePublishing {
		return nil
	}
	if len(msg.Payload) < 1 {
		return nil
	}

	b0 := msg.Payload[0]
	if b0&0x80 != 0 {
		pk, err := parseEnhancedVideo(msg.Payload)
		if err != nil {
			return err
		}
		pk.Timestamp = msg.Timestamp
		pk.StreamID = msg.StreamID
		pk.VideoCodec = VideoCodecEnhanced

		s.dbg("video packet enhanced fourcc=%s ts=%d size=%d", pk.FourCC, msg.Timestamp, len(msg.Payload))
		s.h.OnPacket(s, &pk)
		return nil
	}

	ft := (b0 >> 4) & 0x0F
	vc := VideoCodec(b0 & 0x0F)
	kf := ft == 1
	sh := false
	ct := int32(0)

	if vc == VideoCodecH264 && len(msg.Payload) >= 5 {
		sh = msg.Payload[1] == 0
		u := uint32(msg.Payload[2])<<16 | uint32(msg.Payload[3])<<8 | uint32(msg.Payload[4])
		ct = int32(u)
		// Composition time in AVC packet is signed 24-bit.
		// We sign-extend to int32 so negative offsets stay correct.
		if ct&0x00800000 != 0 {
			ct |= ^int32(0x00FFFFFF)
		}
	}

	fcc := FourCCNone
	vpt := VideoPacketCodedFrames
	if vc == VideoCodecH264 {
		fcc = FourCCAVC
		if sh {
			vpt = VideoPacketSequenceStart
		}
	}

	pk := &Packet{
		Type:             PacketTypeVideo,
		FourCC:           fcc,
		VideoPacketType:  vpt,
		VideoCodec:       vc,
		IsSequenceHeader: sh,
		IsKeyframe:       kf,
		Timestamp:        msg.Timestamp,
		CompositionTime:  ct,
		StreamID:         msg.StreamID,
		Payload:          msg.Payload,
	}
	s.dbg("video packet codec=0x%X keyframe=%t seq_header=%t ts=%d cts=%d size=%d",
		vc, kf, sh, msg.Timestamp, ct, len(msg.Payload))
	s.h.OnPacket(s, pk)
	return nil
}

func (s *Session) sendAck(v uint32) error {
	s.dbg("write ack sequence=%d", v)
	return s.writeControl(3, control.BuildAcknowledgement(v))
}

func (s *Session) replyResult(tx float64, a0 any, a1 any) error {
	pl, err := amf0.EncodeCommand("_result", tx, a0, a1)
	if err != nil {
		return err
	}
	return s.writeCommand(pl)
}

func (s *Session) replyConnectError(tx float64, er error) error {
	inf := map[string]any{
		"level":       "error",
		"code":        "NetConnection.Connect.Rejected",
		"description": er.Error(),
	}
	pl, err := amf0.EncodeCommand("_error", tx, nil, inf)
	if err != nil {
		return err
	}
	return s.writeCommand(pl)
}

func (s *Session) replyFCPublish(vs []any) error {
	d := ""
	if len(vs) >= 4 {
		d = asStr(vs[3])
	}
	inf := map[string]any{
		"code":        "NetStream.Publish.Start",
		"description": d,
	}
	pl, err := amf0.EncodeCommand("onFCPublish", 0, nil, inf)
	if err != nil {
		return err
	}
	return s.writeCommand(pl)
}

func (s *Session) replyPublishBadName() error {
	inf := map[string]any{
		"level":       "error",
		"code":        "NetStream.Publish.BadName",
		"description": "Publish rejected.",
	}
	pl, err := amf0.EncodeCommand("onStatus", 0, nil, inf)
	if err != nil {
		return err
	}
	return s.writeCommandOnStream(1, pl)
}

func (s *Session) writeControl(tid uint8, p []byte) error {
	s.wo.Lock()
	defer s.wo.Unlock()
	if s.cf.WriteTimeout > 0 {
		_ = s.cn.SetWriteDeadline(time.Now().Add(s.cf.WriteTimeout))
	}
	s.dbg("write control type=0x%02X size=%d", tid, len(p))
	return s.cw.WriteControl(tid, p)
}

func (s *Session) writeCommand(p []byte) error {
	s.wo.Lock()
	defer s.wo.Unlock()
	if s.cf.WriteTimeout > 0 {
		_ = s.cn.SetWriteDeadline(time.Now().Add(s.cf.WriteTimeout))
	}
	s.dbg("write command stream=0 size=%d", len(p))
	return s.cw.WriteCommand(p)
}

func (s *Session) writeCommandOnStream(sid uint32, p []byte) error {
	s.wo.Lock()
	defer s.wo.Unlock()
	if s.cf.WriteTimeout > 0 {
		_ = s.cn.SetWriteDeadline(time.Now().Add(s.cf.WriteTimeout))
	}
	s.dbg("write command stream=%d size=%d", sid, len(p))
	return s.cw.WriteCommandOnStream(sid, p)
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) RemoteAddr() net.Addr {
	return s.cn.RemoteAddr()
}

func (s *Session) AppName() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ap
}

func (s *Session) StreamKey() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sk
}

func (s *Session) SetUserData(v any) {
	s.mu.Lock()
	s.ud = v
	s.mu.Unlock()
}

func (s *Session) UserData() any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ud
}

func (s *Session) Close() error {
	var err error
	s.oc.Do(func() {
		s.inf("close requested")
		close(s.ch)
		err = s.cn.Close()
	})
	return err
}

func (s *Session) phaseName() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return phaseName(s.ph)
}

func phaseName(p phase) string {
	switch p {
	case phaseHandshaked:
		return "handshaked"
	case phaseConnecting:
		return "connecting"
	case phasePublishing:
		return "publishing"
	case phaseClosed:
		return "closed"
	default:
		return "unknown"
	}
}

func (s *Session) dbg(msg string, args ...any) {
	s.lg.Debug("session=%s "+msg, append([]any{s.id}, args...)...)
}

func (s *Session) inf(msg string, args ...any) {
	s.lg.Info("session=%s "+msg, append([]any{s.id}, args...)...)
}

func (s *Session) wrn(msg string, args ...any) {
	s.lg.Warn("session=%s "+msg, append([]any{s.id}, args...)...)
}

func (s *Session) err(msg string, args ...any) {
	s.lg.Error("session=%s "+msg, append([]any{s.id}, args...)...)
}

func mkID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}

func isClosedNetErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	// net package returns this text for closed sockets on some platforms.
	return strings.Contains(err.Error(), "use of closed network connection")
}

func asF64(v any) (float64, bool) {
	x, ok := v.(float64)
	return x, ok
}

func asNum(v any) float64 {
	if x, ok := v.(float64); ok {
		return x
	}
	return 0
}

func asStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func toMeta(m map[string]any) Metadata {
	md := Metadata{
		Extra: make(map[string]any),
	}

	for k, v := range m {
		n, ok := v.(float64)
		if !ok {
			md.Extra[k] = v
			continue
		}

		switch k {
		case "videocodecid":
			md.VideoCodecID = n
		case "audiocodecid":
			md.AudioCodecID = n
		case "width":
			md.Width = n
		case "height":
			md.Height = n
		case "framerate":
			md.FrameRate = n
		case "videodatarate":
			md.VideoDataRate = n
		case "audiodatarate":
			md.AudioDataRate = n
		case "audiochannels":
			md.AudioChannels = n
		case "audiosamplerate":
			md.AudioSampleRate = n
		case "duration":
			md.Duration = n
		case "filesize":
			md.FileSize = n
		default:
			md.Extra[k] = v
		}
	}

	return md
}
