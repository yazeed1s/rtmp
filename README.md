# rtmp

RTMP ingest library for Go. Zero dependencies outside stdlib.

It accepts TCP connections from RTMP publishers (OBS, FFmpeg, etc),
speaks the protocol, and gives you media packets through callbacks.
That is all it does. The caller/user should deal with the media packets as they wish.

## Install

```bash
go get github.com/yazeed1s/rtmp
```

## How It Works

```
OBS / FFmpeg / Any encoder
      │  RTMP over TCP
      v
  [ this library ]
      │  callbacks: OnConnect, OnPublish, OnMetadata, OnPacket, OnDisconnect
      v
  [ some code ]
```

You make a server, give it a handler with 5 methods, and call `ListenAndServe`.
The library handles handshake, chunk parsing, AMF0 commands, and flow control.
Your handler receives clean typed events.

## Quick Example

```go
package main

import (
    "log"
    "github.com/yazeed1s/rtmp/rtmp"
)

type handler struct{}

func (handler) OnConnect(s rtmp.Session, info rtmp.ConnectInfo) error {
    log.Printf("connect app=%s", info.App)
    return nil
}

func (handler) OnPublish(s rtmp.Session, info rtmp.PublishInfo) error {
    log.Printf("publish key=%s", info.StreamKey)
    return nil
}

func (handler) OnMetadata(s rtmp.Session, m rtmp.Metadata) {
    log.Printf("metadata %.0fx%.0f", m.Width, m.Height)
}

func (handler) OnPacket(s rtmp.Session, pkt *rtmp.Packet) {
    // pkt.Payload is only valid during this call.
    // call pkt.Clone() if you need to keep it.
}

func (handler) OnDisconnect(s rtmp.Session, err error) {
    log.Printf("disconnect err=%v", err)
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
```

Test it:

```bash
ffmpeg -re -i video.mp4 -c copy -f flv rtmp://127.0.0.1:1935/live/mykey
```

## Config

All fields are optional. Defaults are applied if you leave them zero.

| Field | Default | What it does |
|-------|---------|-------------|
| `ListenAddr` | `":1935"` | TCP address to listen on |
| `MaxConnections` | `0` (no limit) | Max concurrent sessions |
| `HandshakeTimeout` | `10s` | Time limit for RTMP handshake |
| `ReadTimeout` | `0` (none) | Idle timeout per read; 0 means no deadline |
| `WriteTimeout` | `5s` | Timeout for writing responses |
| `MaxChunkSize` | `65536` | Max chunk size we accept from publisher |
| `MaxMessageSize` | `4MB` | Reject messages bigger than this |
| `ReadBufSize` | `4096` | Size of bufio read buffer |
| `WriteBufSize` | `4096` | Size of bufio write buffer |
| `Logger` | no-op | Your logger, must implement `Debug/Info/Warn/Error` |

Example with all fields set:

```go
srv, err := rtmp.NewServer(rtmp.Config{
    ListenAddr:       ":1935",
    MaxConnections:   200,
    HandshakeTimeout: 5 * time.Second,
    ReadTimeout:      30 * time.Second,
    WriteTimeout:     10 * time.Second,
    MaxChunkSize:     65536,
    MaxMessageSize:   4 * 1024 * 1024,
    ReadBufSize:      8192,
    WriteBufSize:     8192,
    Logger:           myLogger,
}, handler{})
```

You do not need to set every field. Any field left at zero gets the default value.
For example, this is enough for most cases:

```go
srv, err := rtmp.NewServer(rtmp.Config{
    ListenAddr:     ":1935",
    MaxConnections: 100,
    Logger:         myLogger,
}, handler{})
```

## Handler Rules

- All 5 methods are required.
- `OnConnect` and `OnPublish` can return error to reject the session.
- `OnPacket` runs on the read goroutine. Do not block it for long.
  If your downstream is slow, clone and send to a channel.
- `pkt.Payload` points into a pool buffer. It is only valid during `OnPacket`.
  Call `pkt.Clone()` before returning if you want to keep it.
- `OnDisconnect` fires exactly once per session, no matter what.

## What It Supports

- Simple handshake (C0/C1/C2, S0/S1/S2)
- All chunk header types (fmt 0/1/2/3), all CSID sizes (1/2/3 byte)
- Extended timestamps
- Protocol control: SetChunkSize, Abort, Acknowledgement, WindowAckSize, SetPeerBandwidth
- User control: StreamBegin, PingRequest/PingResponse
- Commands: connect, releaseStream, FCPublish, createStream, publish, deleteStream, FCUnpublish
- Data messages: @setDataFrame / onMetaData
- Audio (type 8): AAC, MP3
- Video (type 9): H.264 with full FLV tag parsing (keyframe, sequence header, CTS)
- Enhanced RTMP (v2): Full zero-allocation parsing for modern codecs (HEVC, AV1, VP9, Opus). Extracts `FourCC`, packet types, and composition times. Tag envelopes are automatically stripped from payloads.

## What It Does Not Do

- playback (play, seek, pause)
- media decoding (no SPS/PPS parsing, no AudioSpecificConfig)
- file writing (no HLS, no fMP4, no segments)
- transcoding
- authentication (your `OnConnect`/`OnPublish` decides)
- E-RTMPS (planned for v2)
- AMF3

## Examples

See [`examples/`](./examples/) folder:

- [`simple`](./examples/simple/) — minimal server that logs packets
- [`ingest`](./examples/ingest/) — how to use this as the ingest layer in a streaming engine

## Development

```bash
go test ./...
go test -race ./rtmp
go test -run=^$ -bench=. -benchmem ./...
```

See [`BENCHMARKS.md`](./BENCHMARKS.md) for performance numbers.
