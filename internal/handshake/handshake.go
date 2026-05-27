package handshake

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	ver3   = 0x03
	szC1S1 = 1536
	szC2S2 = 1536
)

// DoServer runs plain RTMP handshake for server side.
//
// We support version 3 plain handshake for now
// version 6 will be added later on if needed.
func DoServer(cn net.Conn, to time.Duration) error {
	if to > 0 {
		if err := cn.SetDeadline(time.Now().Add(to)); err != nil {
			return hsErr("set deadline", err)
		}
	}

	var c0 [1]byte
	if _, err := io.ReadFull(cn, c0[:]); err != nil {
		return hsErr("read c0", err)
	}
	if c0[0] != ver3 {
		return fmt.Errorf("bad version: %d", c0[0])
	}

	var c1 [szC1S1]byte
	if _, err := io.ReadFull(cn, c1[:]); err != nil {
		return hsErr("read c1", err)
	}
	c1t := binary.BigEndian.Uint32(c1[0:4])
	_ = c1[4:8]
	c1r := c1[8:szC1S1]

	var s1 [szC1S1]byte
	binary.BigEndian.PutUint32(s1[0:4], nowMS())
	s1[4], s1[5], s1[6], s1[7] = 0, 0, 0, 0
	if _, err := rand.Read(s1[8:]); err != nil {
		return hsErr("build s1 random", err)
	}

	var s2 [szC2S2]byte
	binary.BigEndian.PutUint32(s2[0:4], c1t)
	binary.BigEndian.PutUint32(s2[4:8], nowMS())
	copy(s2[8:], c1r)

	var out [1 + szC1S1 + szC2S2]byte
	out[0] = ver3
	copy(out[1:1+szC1S1], s1[:])
	copy(out[1+szC1S1:], s2[:])

	n, err := cn.Write(out[:])
	if err != nil {
		return hsErr("write s0s1s2", err)
	}
	if n != len(out) {
		return hsErr("write s0s1s2", io.ErrShortWrite)
	}

	var c2 [szC2S2]byte
	if _, err := io.ReadFull(cn, c2[:]); err != nil {
		return hsErr("read c2", err)
	}
	if !bytes.Equal(c2[8:], s1[8:]) {
		// Some publishers send bad C2 random echo.
		// We keep compatibility and continue.
	}

	if err := cn.SetDeadline(time.Time{}); err != nil {
		return hsErr("clear deadline", err)
	}
	return nil
}

func hsErr(msg string, err error) error {
	return fmt.Errorf("%s: %w", msg, err)
}

func nowMS() uint32 {
	return uint32(time.Now().UnixMilli())
}
