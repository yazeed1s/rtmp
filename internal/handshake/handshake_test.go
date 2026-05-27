package handshake

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestDoServerOK(t *testing.T) {
	cl, sv := net.Pipe()
	defer cl.Close()
	defer sv.Close()

	ch := make(chan error, 1)
	go func() {
		ch <- DoServer(sv, time.Second)
	}()

	c1, c1t := mkC1()
	if _, err := cl.Write(append([]byte{0x03}, c1[:]...)); err != nil {
		t.Fatalf("write c0c1: %v", err)
	}

	var srv [1 + szC1S1 + szC2S2]byte
	if _, err := io.ReadFull(cl, srv[:]); err != nil {
		t.Fatalf("read s0s1s2: %v", err)
	}
	if srv[0] != 0x03 {
		t.Fatalf("s0 version mismatch: got=%d want=3", srv[0])
	}

	s1 := srv[1 : 1+szC1S1]
	s2 := srv[1+szC1S1:]
	if got := binary.BigEndian.Uint32(s2[0:4]); got != c1t {
		t.Fatalf("s2 c1time echo mismatch: got=%d want=%d", got, c1t)
	}

	var c2 [szC2S2]byte
	copy(c2[0:4], s1[0:4])
	binary.BigEndian.PutUint32(c2[4:8], 0)
	copy(c2[8:], s1[8:])
	if _, err := cl.Write(c2[:]); err != nil {
		t.Fatalf("write c2: %v", err)
	}

	if err := <-ch; err != nil {
		t.Fatalf("server handshake error: %v", err)
	}
}

func TestDoServerBadVersion(t *testing.T) {
	cl, sv := net.Pipe()
	defer cl.Close()
	defer sv.Close()

	ch := make(chan error, 1)
	go func() {
		ch <- DoServer(sv, time.Second)
	}()

	if _, err := cl.Write([]byte{0x00}); err != nil {
		t.Fatalf("write c0: %v", err)
	}

	err := <-ch
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "bad version") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDoServerTimeout(t *testing.T) {
	cl, sv := net.Pipe()
	defer cl.Close()
	defer sv.Close()

	ch := make(chan error, 1)
	go func() {
		ch <- DoServer(sv, 20*time.Millisecond)
	}()

	select {
	case err := <-ch:
		if err == nil {
			t.Fatal("expected timeout error")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server result")
	}
}

func TestDoServerBadC2StillOK(t *testing.T) {
	cl, sv := net.Pipe()
	defer cl.Close()
	defer sv.Close()

	ch := make(chan error, 1)
	go func() {
		ch <- DoServer(sv, time.Second)
	}()

	c1, _ := mkC1()
	if _, err := cl.Write(append([]byte{0x03}, c1[:]...)); err != nil {
		t.Fatalf("write c0c1: %v", err)
	}

	var srv [1 + szC1S1 + szC2S2]byte
	if _, err := io.ReadFull(cl, srv[:]); err != nil {
		t.Fatalf("read s0s1s2: %v", err)
	}

	var c2 [szC2S2]byte
	copy(c2[0:4], srv[1:5])
	for i := 8; i < len(c2); i++ {
		c2[i] = 0xAA
	}
	if _, err := cl.Write(c2[:]); err != nil {
		t.Fatalf("write c2: %v", err)
	}

	if err := <-ch; err != nil {
		t.Fatalf("server handshake error: %v", err)
	}
}

func mkC1() ([szC1S1]byte, uint32) {
	var c1 [szC1S1]byte
	tm := uint32(0x01020304)
	binary.BigEndian.PutUint32(c1[0:4], tm)
	c1[4], c1[5], c1[6], c1[7] = 0, 0, 0, 0
	for i := 8; i < len(c1); i++ {
		c1[i] = byte(i % 251)
	}
	return c1, tm
}
