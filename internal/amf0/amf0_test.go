package amf0

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestDecodeNumber(t *testing.T) {
	// marker(0x00) + float64(1.5) big-endian
	d := []byte{
		0x00,
		0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	v, err := Decode(d)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(v) != 1 {
		t.Fatalf("len mismatch: got=%d want=1", len(v))
	}
	f, ok := v[0].(float64)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=float64", v[0])
	}
	if f != 1.5 {
		t.Fatalf("value mismatch: got=%v want=1.5", f)
	}
}

func TestDecodeBoolean(t *testing.T) {
	d := []byte{0x01, 0x01}
	v, err := Decode(d)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	b, ok := v[0].(bool)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=bool", v[0])
	}
	if !b {
		t.Fatal("value mismatch: got=false want=true")
	}
}

func TestDecodeString(t *testing.T) {
	d := []byte{0x02, 0x00, 0x03, 'a', 'b', 'c'}
	v, err := Decode(d)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	s, ok := v[0].(string)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=string", v[0])
	}
	if s != "abc" {
		t.Fatalf("value mismatch: got=%q want=%q", s, "abc")
	}
}

func TestDecodeObject(t *testing.T) {
	// object with one key: "x" => 2.0, then end marker
	d := []byte{
		0x03,
		0x00, 0x01, 'x',
		0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x09,
	}
	v, err := Decode(d)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	o, ok := v[0].(map[string]any)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=map[string]any", v[0])
	}
	f, ok := o["x"].(float64)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=float64", o["x"])
	}
	if f != 2.0 {
		t.Fatalf("value mismatch: got=%v want=2.0", f)
	}
}

func TestDecodeNestedObject(t *testing.T) {
	// {"a":{"b":"ok"}}
	d := []byte{
		0x03,
		0x00, 0x01, 'a',
		0x03,
		0x00, 0x01, 'b',
		0x02, 0x00, 0x02, 'o', 'k',
		0x00, 0x00, 0x09,
		0x00, 0x00, 0x09,
	}
	v, err := Decode(d)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	o, ok := v[0].(map[string]any)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=map[string]any", v[0])
	}
	in, ok := o["a"].(map[string]any)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=map[string]any", o["a"])
	}
	s, ok := in["b"].(string)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=string", in["b"])
	}
	if s != "ok" {
		t.Fatalf("value mismatch: got=%q want=%q", s, "ok")
	}
}

func TestDecodeECMAArray(t *testing.T) {
	// ecma array count=1, but parser reads until end marker
	d := []byte{
		0x08,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x01, 'k',
		0x02, 0x00, 0x01, 'v',
		0x00, 0x00, 0x09,
	}
	v, err := Decode(d)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	o, ok := v[0].(map[string]any)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=map[string]any", v[0])
	}
	s, ok := o["k"].(string)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=string", o["k"])
	}
	if s != "v" {
		t.Fatalf("value mismatch: got=%q want=%q", s, "v")
	}
}

func TestDecodeStrictArray(t *testing.T) {
	// [3.0, true]
	d := []byte{
		0x0A,
		0x00, 0x00, 0x00, 0x02,
		0x00, 0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x01,
	}
	v, err := Decode(d)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	a, ok := v[0].([]any)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=[]any", v[0])
	}
	if len(a) != 2 {
		t.Fatalf("len mismatch: got=%d want=2", len(a))
	}
	f, ok := a[0].(float64)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=float64", a[0])
	}
	if math.Abs(f-3.0) > 0 {
		t.Fatalf("value mismatch: got=%v want=3.0", f)
	}
	b, ok := a[1].(bool)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=bool", a[1])
	}
	if !b {
		t.Fatal("value mismatch: got=false want=true")
	}
}

func TestDecodeStrictArrayHugeCount(t *testing.T) {
	// count says huge number but buffer has no element bytes.
	d := []byte{
		0x0A,
		0xFF, 0xFF, 0xFF, 0xFF,
	}
	_, err := Decode(d)
	if err == nil {
		t.Fatal("expected error for oversized strict array count")
	}
}

func TestObjectEndMarker(t *testing.T) {
	d := []byte{
		0x03,
		0x00, 0x00, 0x09,
	}
	v, err := Decode(d)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	o, ok := v[0].(map[string]any)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=map[string]any", v[0])
	}
	if len(o) != 0 {
		t.Fatalf("len mismatch: got=%d want=0", len(o))
	}
}

func TestUnknownMarker(t *testing.T) {
	d := []byte{0x7F}
	_, err := Decode(d)
	if err == nil {
		t.Fatal("expected error for unknown marker")
	}
}

func TestEmptyInput(t *testing.T) {
	v, err := Decode(nil)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(v) != 0 {
		t.Fatalf("len mismatch: got=%d want=0", len(v))
	}
}

func TestEncodeNumber(t *testing.T) {
	b, err := Encode(1.5)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	ex := []byte{
		0x00,
		0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	if !bytes.Equal(b, ex) {
		t.Fatalf("bytes mismatch: got=%x want=%x", b, ex)
	}
}

func TestEncodeBoolean(t *testing.T) {
	b, err := Encode(true)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	ex := []byte{0x01, 0x01}
	if !bytes.Equal(b, ex) {
		t.Fatalf("bytes mismatch: got=%x want=%x", b, ex)
	}
}

func TestEncodeString(t *testing.T) {
	b, err := Encode("abc")
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	ex := []byte{0x02, 0x00, 0x03, 'a', 'b', 'c'}
	if !bytes.Equal(b, ex) {
		t.Fatalf("bytes mismatch: got=%x want=%x", b, ex)
	}
}

func TestEncodeLongString(t *testing.T) {
	s := strings.Repeat("a", 65536)
	b, err := Encode(s)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	if len(b) != 1+4+len(s) {
		t.Fatalf("len mismatch: got=%d want=%d", len(b), 1+4+len(s))
	}
	if b[0] != 0x0C {
		t.Fatalf("marker mismatch: got=0x%02x want=0x0c", b[0])
	}
	if b[1] != 0x00 || b[2] != 0x01 || b[3] != 0x00 || b[4] != 0x00 {
		t.Fatalf("length bytes mismatch: got=%x", b[1:5])
	}
}

func TestEncodeObject(t *testing.T) {
	b, err := Encode(map[string]any{"x": 2.0})
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	ex := []byte{
		0x03,
		0x00, 0x01, 'x',
		0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x09,
	}
	if !bytes.Equal(b, ex) {
		t.Fatalf("bytes mismatch: got=%x want=%x", b, ex)
	}
}

func TestEncodeNil(t *testing.T) {
	b, err := Encode(nil)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	ex := []byte{0x05}
	if !bytes.Equal(b, ex) {
		t.Fatalf("bytes mismatch: got=%x want=%x", b, ex)
	}
}

func TestEncodeUndefined(t *testing.T) {
	b, err := Encode(Undefined)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	ex := []byte{0x06}
	if !bytes.Equal(b, ex) {
		t.Fatalf("bytes mismatch: got=%x want=%x", b, ex)
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	in := []any{
		"_result",
		1.0,
		nil,
		map[string]any{
			"level": "status",
			"code":  "ok",
		},
		true,
	}

	b, err := Encode(in...)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	out, err := Decode(b)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("len mismatch: got=%d want=%d", len(out), len(in))
	}

	if out[0] != "_result" {
		t.Fatalf("value mismatch: got=%v want=_result", out[0])
	}
	if out[1] != 1.0 {
		t.Fatalf("value mismatch: got=%v want=1", out[1])
	}
	if out[2] != nil {
		t.Fatalf("value mismatch: got=%v want=nil", out[2])
	}
	o, ok := out[3].(map[string]any)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=map[string]any", out[3])
	}
	if o["level"] != "status" || o["code"] != "ok" {
		t.Fatalf("object mismatch: got=%v", o)
	}
	if out[4] != true {
		t.Fatalf("value mismatch: got=%v want=true", out[4])
	}
}

func TestEncodeCommand(t *testing.T) {
	b, err := EncodeCommand("_result", 1.0, nil, map[string]any{"level": "status"})
	if err != nil {
		t.Fatalf("encode command error: %v", err)
	}

	v, err := Decode(b)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(v) != 4 {
		t.Fatalf("len mismatch: got=%d want=4", len(v))
	}
	if v[0] != "_result" {
		t.Fatalf("value mismatch: got=%v want=_result", v[0])
	}
	if v[1] != 1.0 {
		t.Fatalf("value mismatch: got=%v want=1.0", v[1])
	}
	if v[2] != nil {
		t.Fatalf("value mismatch: got=%v want=nil", v[2])
	}
	o, ok := v[3].(map[string]any)
	if !ok {
		t.Fatalf("type mismatch: got=%T want=map[string]any", v[3])
	}
	if o["level"] != "status" {
		t.Fatalf("value mismatch: got=%v want=status", o["level"])
	}
}

func TestEncodeUnsupported(t *testing.T) {
	_, err := Encode(1)
	if err == nil {
		t.Fatal("expected error")
	}
}
