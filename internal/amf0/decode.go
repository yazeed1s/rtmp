package amf0

import (
	"fmt"
	"math"
)

const (
	markerNumber      byte = 0x00
	markerBoolean     byte = 0x01
	markerString      byte = 0x02
	markerObject      byte = 0x03
	markerNull        byte = 0x05
	markerUndefined   byte = 0x06
	markerECMAArray   byte = 0x08
	markerObjectEnd   byte = 0x09
	markerStrictArray byte = 0x0A
)

type decoder struct {
	data []byte
	pos  int
}

// Decode reads AMF0 values from a byte slice.
//
// RTMP command messages (type 20) and data messages (type 18) carry AMF0
// values one after another. We decode until we consume all bytes.
func Decode(data []byte) ([]any, error) {
	d := decoder{data: data}
	out := make([]any, 0)

	for d.pos < len(d.data) {
		v, err := readValue(&d)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}

	return out, nil
}

func readValue(d *decoder) (any, error) {
	m, err := d.readByte()
	if err != nil {
		return nil, err
	}

	switch m {
	case markerNumber:
		// AMF0 Number is IEEE-754 float64 in big-endian.
		u, err := d.readU64()
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(u), nil

	case markerBoolean:
		// AMF0 Boolean uses one byte: 0 is false, non-zero is true.
		b, err := d.readByte()
		if err != nil {
			return nil, err
		}
		return b != 0, nil

	case markerString:
		// AMF0 String has 2-byte big-endian length, then string bytes.
		n, err := d.readU16()
		if err != nil {
			return nil, err
		}
		s, err := d.readStringN(int(n))
		if err != nil {
			return nil, err
		}
		return s, nil

	case markerObject:
		// AMF0 Object is key/value pairs, closed by 0x00 0x00 0x09.
		return readObject(d)

	case markerNull:
		// In Go side we map both null and undefined as nil for this v1 parser.
		return nil, nil

	case markerUndefined:
		return nil, nil

	case markerECMAArray:
		// ECMAArray starts with 4-byte count, but many encoders mismatch count.
		// RTMP ecosystem usually reads until object end marker, not by count.
		_, err := d.readU32()
		if err != nil {
			return nil, err
		}
		return readObject(d)

	case markerStrictArray:
		// StrictArray has exact count. We decode exactly N values in order.
		n, err := d.readU32()
		if err != nil {
			return nil, err
		}
		// Safety; each value needs at least 1 marker byte.
		// If count is bigger than remaining bytes, input is invalid.
		rm := len(d.data) - d.pos
		if n > uint32(rm) {
			return nil, fmt.Errorf("amf0: strict array too large: count=%d remaining=%d", n, rm)
		}
		a := make([]any, 0, int(n))
		for i := uint32(0); i < n; i++ {
			v, err := readValue(d)
			if err != nil {
				return nil, err
			}
			a = append(a, v)
		}
		return a, nil

	default:
		return nil, fmt.Errorf("amf0: unsupported type 0x%02x", m)
	}
}

func readObject(d *decoder) (map[string]any, error) {
	o := make(map[string]any)

	for {
		n, err := d.readU16()
		if err != nil {
			return nil, err
		}

		// AMF0 object end is: empty key (len=0) + markerObjectEnd (0x09).
		if n == 0 {
			m, err := d.readByte()
			if err != nil {
				return nil, err
			}
			if m == markerObjectEnd {
				return o, nil
			}
			return nil, fmt.Errorf("amf0: object end not found")
		}

		k, err := d.readStringN(int(n))
		if err != nil {
			return nil, err
		}

		v, err := readValue(d)
		if err != nil {
			return nil, err
		}
		o[k] = v
	}
}

func (d *decoder) readByte() (byte, error) {
	if d.pos >= len(d.data) {
		return 0, fmt.Errorf("amf0: buffer too short")
	}
	b := d.data[d.pos]
	d.pos++
	return b, nil
}

func (d *decoder) readU16() (uint16, error) {
	if len(d.data)-d.pos < 2 {
		return 0, fmt.Errorf("amf0: buffer too short")
	}
	v := uint16(d.data[d.pos])<<8 | uint16(d.data[d.pos+1])
	d.pos += 2
	return v, nil
}

func (d *decoder) readU32() (uint32, error) {
	if len(d.data)-d.pos < 4 {
		return 0, fmt.Errorf("amf0: buffer too short")
	}
	v := uint32(d.data[d.pos])<<24 |
		uint32(d.data[d.pos+1])<<16 |
		uint32(d.data[d.pos+2])<<8 |
		uint32(d.data[d.pos+3])
	d.pos += 4
	return v, nil
}

func (d *decoder) readU64() (uint64, error) {
	if len(d.data)-d.pos < 8 {
		return 0, fmt.Errorf("amf0: buffer too short")
	}
	v := uint64(d.data[d.pos])<<56 |
		uint64(d.data[d.pos+1])<<48 |
		uint64(d.data[d.pos+2])<<40 |
		uint64(d.data[d.pos+3])<<32 |
		uint64(d.data[d.pos+4])<<24 |
		uint64(d.data[d.pos+5])<<16 |
		uint64(d.data[d.pos+6])<<8 |
		uint64(d.data[d.pos+7])
	d.pos += 8
	return v, nil
}

func (d *decoder) readStringN(n int) (string, error) {
	if n < 0 || len(d.data)-d.pos < n {
		return "", fmt.Errorf("amf0: buffer too short")
	}
	s := string(d.data[d.pos : d.pos+n])
	d.pos += n
	return s, nil
}
