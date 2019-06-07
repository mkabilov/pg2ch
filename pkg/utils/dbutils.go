package utils

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	// OutputPlugin contains logical decoder plugin name
	OutputPlugin = "pgoutput"

	copyNull = 'N'

	IstoreOID    = OID(34111)
	BigIstoreOID = OID(34117)
)

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

//QuoteLiteral quotes string literal
func QuoteLiteral(str string) string {
	needsEscapeChar := false
	res := ""
	for _, r1 := range str {
		switch r1 {
		case '\\':
			res += `\\`
		case '\t':
			res += `\t`
			needsEscapeChar = true
		case '\r':
			res += `\r`
			needsEscapeChar = true
		case '\n':
			res += `\n`
			needsEscapeChar = true
		default:
			res += string(r1)
		}
	}

	if needsEscapeChar {
		return `E'` + res + `'`
	}

	return `'` + res + `'`
}

//DecodeCopy extracts fields from the postgresql text copy format
// based on DecodeCopy from https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/copy.go
func DecodeCopy(in []byte) ([]sql.NullString, error) {
	result := make([]sql.NullString, 0)

	isValid := true
	str := &strings.Builder{}
	for i, n := 0, len(in); i < n; i++ {
		if in[i] == '\t' {
			result = append(result, sql.NullString{Valid: isValid, String: str.String()})
			isValid = true
			str.Reset()
			continue
		} else if in[i] == '\n' {
			result = append(result, sql.NullString{Valid: isValid, String: str.String()})
			return result, nil
		} else if in[i] != '\\' {
			str.WriteByte(in[i])
			continue
		}
		i++
		if i >= n {
			return nil, fmt.Errorf("unknown escape sequence: %q", in[i-1:])
		}

		ch := in[i]
		if ch == copyNull {
			isValid = false
			continue
		}

		if decodedChar, ok := decodeMap[ch]; ok {
			str.WriteByte(decodedChar)
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
			str.WriteByte(digit)

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
			str.WriteByte(digit)

			continue
		}

		return nil, fmt.Errorf("unknown escape sequence: %q", in[i-1:i+1])
	}

	return result, nil
}

func ParseIstore(str string) (keys, values []int, err error) {
	keys = make([]int, 0)
	values = make([]int, 0)
	for _, pair := range strings.Split(str, ",") {
		var key, value int
		n, err := fmt.Sscanf(strings.TrimLeft(pair, " "), `"%d"=>"%d"`, &key, &value)
		if err != nil || n != 2 {
			return nil, nil, fmt.Errorf("could not parse istore: %v(%d)", err, n)
		}

		keys = append(keys, key)
		values = append(values, value)
	}

	return
}

func SplitIstore(str string) (keys, values []string, err error) {
	keys = make([]string, 0)
	values = make([]string, 0)

	isKey := true
	tmpStr := strings.Builder{}
	isNum := false
	for _, c := range str {
		switch c {
		case '"':
			isNum = !isNum
			if !isNum {
				if isKey {
					keys = append(keys, tmpStr.String())
				} else {
					values = append(values, tmpStr.String())
					isKey = true
				}
			} else {
				tmpStr.Reset()
			}
		case '=':
			isKey = false
		case '>':
		default:
			tmpStr.WriteRune(c)
		}
	}
	if len(keys) != len(values) {
		err = fmt.Errorf("wrong istore")
	}

	return
}
