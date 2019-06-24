package pgutils

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/mkabilov/pg2ch/pkg/message"
)

const copyNull = 'N'

var bytesBufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	}}

var decodeMap = map[byte]byte{
	'b':  '\b',
	'f':  '\f',
	'n':  '\n',
	'r':  '\r',
	't':  '\t',
	'v':  '\v',
	'\\': '\\',
}

func decodeDigit(c byte, onlyOctal bool) (byte, bool) {
	switch {
	case c >= '0' && c <= '7':
		return c - '0', true
	case !onlyOctal && c >= '8' && c <= '9':
		return c - '0', true
	case !onlyOctal && c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case !onlyOctal && c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

func decodeOctDigit(c byte) (byte, bool) { return decodeDigit(c, true) }
func decodeHexDigit(c byte) (byte, bool) { return decodeDigit(c, false) }

// DecodeCopyToTuples decodes line from the text pg copy command into a array of tuples
func DecodeCopyToTuples(in []byte) (message.Row, error) {
	result := make(message.Row, 0)

	tupleKind := message.TupleText
	colBuf := bytesBufPool.Get().(*bytes.Buffer)
	defer bytesBufPool.Put(&colBuf)

	for i, n := 0, len(in); i < n; i++ {
		if in[i] == '\t' {
			t := make([]byte, colBuf.Len())
			copy(t, colBuf.Bytes())

			result = append(result, message.Tuple{Kind: tupleKind, Value: t})
			tupleKind = message.TupleText
			colBuf.Reset()
			continue
		} else if in[i] == '\n' {
			t := make([]byte, colBuf.Len())
			copy(t, colBuf.Bytes())

			result = append(result, message.Tuple{Kind: tupleKind, Value: t})
			return result, nil
		} else if in[i] != '\\' {
			colBuf.WriteByte(in[i])
			continue
		}
		i++
		if i >= n {
			return nil, fmt.Errorf("unknown escape sequence: %q", in[i-1:])
		}

		ch := in[i]
		if ch == copyNull {
			tupleKind = message.TupleNull
			continue
		}

		if decodedChar, ok := decodeMap[ch]; ok {
			colBuf.WriteByte(decodedChar)
			continue
		}

		if ch == 'x' {
			// \x can be followed by 1 or 2 hex digits.
			i++
			if i >= n {
				return nil, fmt.Errorf("unknown escape sequence: %q", in[i-2:])
			}

			ch = in[i]
			digit, ok := decodeHexDigit(ch)
			if !ok {
				return nil, fmt.Errorf("unknown escape sequence: %q", in[i-2:i])
			}
			if i+1 < n {
				if v, ok := decodeHexDigit(in[i+1]); ok {
					i++
					digit <<= 4
					digit += v
				}
			}
			colBuf.WriteByte(digit)

			continue
		}

		if ch >= '0' && ch <= '7' {
			digit, _ := decodeOctDigit(ch)
			// 1 to 2 more octal digits follow.
			if i+1 < n {
				if v, ok := decodeOctDigit(in[i+1]); ok {
					i++
					digit <<= 3
					digit += v
				}
			}
			if i+1 < n {
				if v, ok := decodeOctDigit(in[i+1]); ok {
					i++
					digit <<= 3
					digit += v
				}
			}
			colBuf.WriteByte(digit)

			continue
		}

		return nil, fmt.Errorf("unknown escape sequence: %q", in[i-1:i+1])
	}

	return result, nil
}
