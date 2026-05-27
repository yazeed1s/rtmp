package amf0

import (
	"fmt"
	"math"
	"sort"
)

const (
	markerLongString byte = 0x0C
)

type undefined struct{}

// Undefined is AMF0 undefined value.
//
// We need this sentinel for commands like releaseStream response where
// RTMP servers send undefined as the 3rd argument.
var Undefined = undefined{}

// Encode writes one or more AMF0 values to bytes.
//
// RTMP command and data payloads are list of AMF0 values in order.
func Encode(vs ...any) ([]byte, error) {
	b := make([]byte, 0, 128)

	for _, v := range vs {
		if err := writeValue(&b, v); err != nil {
			return nil, err
		}
	}

	return b, nil
}

func writeValue(b *[]byte, v any) error {
	switch x := v.(type) {
	case float64:
		// AMF0 Number marker + IEEE-754 float64 big-endian bytes.
		*b = append(*b, markerNumber)
		putU64(b, math.Float64bits(x))
		return nil

	case bool:
		*b = append(*b, markerBoolean)
		if x {
			*b = append(*b, 0x01)
		} else {
			*b = append(*b, 0x00)
		}
		return nil

	case string:
		// Short String uses 2-byte length. Long String uses 4-byte length.
		if len(x) > 65535 {
			*b = append(*b, markerLongString)
			putU32(b, uint32(len(x)))
			*b = append(*b, []byte(x)...)
			return nil
		}

		*b = append(*b, markerString)
		putU16(b, uint16(len(x)))
		*b = append(*b, []byte(x)...)
		return nil

	case map[string]any:
		// AMF0 Object is key/value list and ends with 0x00 0x00 0x09.
		*b = append(*b, markerObject)

		ks := make([]string, 0, len(x))
		for k := range x {
			ks = append(ks, k)
		}
		sort.Strings(ks)

		for _, k := range ks {
			putU16(b, uint16(len(k)))
			*b = append(*b, []byte(k)...)
			if err := writeValue(b, x[k]); err != nil {
				return err
			}
		}

		*b = append(*b, 0x00, 0x00, markerObjectEnd)
		return nil

	case nil:
		*b = append(*b, markerNull)
		return nil

	case undefined:
		*b = append(*b, markerUndefined)
		return nil
	}

	return fmt.Errorf("amf0: cannot encode %T", v)
}

// EncodeCommand builds AMF0 command payload:
// [command name string, transaction id number, args...]
func EncodeCommand(nm string, tx float64, as ...any) ([]byte, error) {
	vs := make([]any, 0, 2+len(as))
	vs = append(vs, nm, tx)
	vs = append(vs, as...)
	return Encode(vs...)
}

func putU16(b *[]byte, v uint16) {
	*b = append(*b, byte(v>>8), byte(v))
}

func putU32(b *[]byte, v uint32) {
	*b = append(*b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func putU64(b *[]byte, v uint64) {
	*b = append(
		*b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v),
	)
}
