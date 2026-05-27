package chunk

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

const (
	// RTMP default outbound chunk size starts at 128 bytes until changed by Set Chunk Size.
	defaultChunkSize = 128
	typeCommandAMF0  = 0x14
)

type Writer struct {
	w  *bufio.Writer
	cs uint32
}

func NewWriter(w *bufio.Writer) *Writer {
	return &Writer{
		w:  w,
		cs: defaultChunkSize,
	}
}

func (w *Writer) SetChunkSize(sz uint32) {
	if sz == 0 {
		return
	}
	w.cs = sz
}

func (w *Writer) WriteMessage(csid uint32, tid uint8, sid uint32, ts uint32, p []byte) error {
	if len(p) > 0x00FFFFFF {
		return fmt.Errorf("chunk: payload too large for rtmp header: %d", len(p))
	}

	ex := ts >= 0x00FFFFFF

	// First chunk is fmt0 because it carries full message header fields.
	h0, err := buildFmt0Header(csid, ts, uint32(len(p)), tid, sid, ex)
	if err != nil {
		return err
	}
	if _, err := w.w.Write(h0); err != nil {
		return err
	}

	n := minInt(int(w.cs), len(p))
	if n > 0 {
		if _, err := w.w.Write(p[:n]); err != nil {
			return err
		}
	}

	pos := n
	for pos < len(p) {
		// Continuation chunks use fmt3.
		// If previous fmt0 had extended timestamp, fmt3 also carries ext timestamp.
		h3, err := buildFmt3Header(csid, ts, ex)
		if err != nil {
			return err
		}
		if _, err := w.w.Write(h3); err != nil {
			return err
		}

		m := minInt(int(w.cs), len(p)-pos)
		if _, err := w.w.Write(p[pos : pos+m]); err != nil {
			return err
		}
		pos += m
	}

	return w.w.Flush()
}

func (w *Writer) WriteControl(tid uint8, p []byte) error {
	return w.WriteMessage(2, tid, 0, 0, p)
}

func (w *Writer) WriteCommand(p []byte) error {
	return w.WriteMessage(3, typeCommandAMF0, 0, 0, p)
}

func (w *Writer) WriteCommandOnStream(sid uint32, p []byte) error {
	return w.WriteMessage(5, typeCommandAMF0, sid, 0, p)
}

func buildFmt0Header(csid uint32, ts uint32, ml uint32, tid uint8, sid uint32, ex bool) ([]byte, error) {
	bh, err := buildBasicHeader(0, csid)
	if err != nil {
		return nil, err
	}

	h := make([]byte, 0, len(bh)+11+4)
	h = append(h, bh...)

	if ex {
		h = append(h, 0xFF, 0xFF, 0xFF)
	} else {
		h = append(h, byte(ts>>16), byte(ts>>8), byte(ts))
	}

	h = append(h, byte(ml>>16), byte(ml>>8), byte(ml))
	h = append(h, tid)

	// RTMP spec detail: message stream id in fmt0 is little-endian.
	var sidb [4]byte
	binary.LittleEndian.PutUint32(sidb[:], sid)
	h = append(h, sidb[:]...)

	if ex {
		var ext [4]byte
		binary.BigEndian.PutUint32(ext[:], ts)
		h = append(h, ext[:]...)
	}

	return h, nil
}

func buildFmt3Header(csid uint32, ts uint32, ex bool) ([]byte, error) {
	bh, err := buildBasicHeader(3, csid)
	if err != nil {
		return nil, err
	}
	if !ex {
		return bh, nil
	}

	h := make([]byte, 0, len(bh)+4)
	h = append(h, bh...)
	var ext [4]byte
	binary.BigEndian.PutUint32(ext[:], ts)
	h = append(h, ext[:]...)
	return h, nil
}

func buildBasicHeader(fm uint8, csid uint32) ([]byte, error) {
	if fm > 3 {
		return nil, fmt.Errorf("chunk: invalid fmt: %d", fm)
	}

	if csid >= 2 && csid <= 63 {
		return []byte{byte(fm<<6) | byte(csid)}, nil
	}

	if csid >= 64 && csid <= 319 {
		return []byte{byte(fm << 6), byte(csid - 64)}, nil
	}

	if csid >= 64 && csid <= 65599 {
		v := csid - 64
		return []byte{byte(fm<<6) | 1, byte(v), byte(v >> 8)}, nil
	}

	return nil, fmt.Errorf("chunk: csid out of range: %d", csid)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
