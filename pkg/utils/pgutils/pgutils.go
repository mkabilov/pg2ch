package pgutils

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
)

const (
	// OutputPlugin contains logical decoder plugin name
	OutputPlugin = "pgoutput"

	lowerhex = "0123456789abcdef"
	copyNull = 'N'
)

var bytesBufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer([]byte{})
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

func IstoreToArrays(str []byte) []byte {
	tmpStr := *bytesBufPool.Get().(*bytes.Buffer)
	defer func() {
		tmpStr.Reset()
		bytesBufPool.Put(&tmpStr)
	}()

	keysBuf := bytes.NewBuffer([]byte{'['})
	valuesBuf := bytes.NewBuffer([]byte{'['})

	i := 0
	isKey := true
	isNum := false
	for _, c := range str {
		switch c {
		case '"':
			if isNum {
				if isKey {
					if i > 1 {
						keysBuf.WriteByte(',')
					}
					keysBuf.Write(tmpStr.Bytes())
				} else {
					if i > 1 {
						valuesBuf.WriteByte(',')
					}
					valuesBuf.Write(tmpStr.Bytes())
					isKey = true
				}
			} else {
				tmpStr.Reset()
				if isKey {
					i++
				}
			}
			isNum = !isNum
		case '=':
			isKey = false
		case '>':
		case ' ':
		case ',':
		default:
			tmpStr.WriteByte(c)
		}
	}
	keysBuf.WriteString("]\t")
	keysBuf.Write(valuesBuf.Bytes())
	keysBuf.WriteByte(']')

	return keysBuf.Bytes()
}

//TODO check istore key value
func IstoreValues(str []byte, min, max int) []byte {
	tmpStr := *bytesBufPool.Get().(*bytes.Buffer)
	defer func() {
		tmpStr.Reset()
		bytesBufPool.Put(&tmpStr)
	}()

	values := make([][]byte, max-min+1)

	isKey := true
	isNum := false
	curKey := 0
	for _, c := range str {
		switch c {
		case '"':
			if isNum {
				if isKey {
					curKey, _ = strconv.Atoi(tmpStr.String())
				} else {
					if curKey <= max {
						values[curKey-min] = make([]byte, tmpStr.Len())
						copy(values[curKey-min], tmpStr.Bytes())
					}
					isKey = true
				}
			} else {
				tmpStr.Reset()
			}
			isNum = !isNum
		case '=':
			isKey = false
		case '>':
		case ' ':
		case ',':
		default:
			tmpStr.WriteByte(c)
		}
	}
	res := make([]byte, 0)
	for i, v := range values {
		if i > 0 {
			res = append(res, '\t')
		}
		if v == nil {
			res = append(res, `\N`...)
		} else {
			res = append(res, v...)
		}
	}

	return res
}

func Quote(str string) string {
	colBuf := *bytesBufPool.Get().(*bytes.Buffer)
	defer func() {
		colBuf.Reset()
		bytesBufPool.Put(&colBuf)
	}()

	var runeTmp [utf8.UTFMax]byte

	for _, r := range []rune(str) {
		if strconv.IsPrint(r) {
			n := utf8.EncodeRune(runeTmp[:], r)
			colBuf.Write(runeTmp[:n])
			continue
		}

		switch r {
		case '\a':
			colBuf.WriteString(`\a`)
		case '\b':
			colBuf.WriteString(`\b`)
		case '\f':
			colBuf.WriteString(`\f`)
		case '\n':
			colBuf.WriteString(`\n`)
		case '\r':
			colBuf.WriteString(`\r`)
		case '\t':
			colBuf.WriteString(`\t`)
		case '\v':
			colBuf.WriteString(`\v`)
		default:
			switch {
			case r < ' ':
				colBuf.WriteString(`\x`)
				colBuf.WriteByte(lowerhex[byte(r)>>4])
				colBuf.WriteByte(lowerhex[byte(r)&0xF])
			case r > utf8.MaxRune:
				r = 0xFFFD
				fallthrough
			case r < 0x10000:
				colBuf.WriteString(`\u`)
				for s := 12; s >= 0; s -= 4 {
					colBuf.WriteByte(lowerhex[r>>uint(s)&0xF])
				}
			default:
				colBuf.WriteString(`\U`)
				for s := 28; s >= 0; s -= 4 {
					colBuf.WriteByte(lowerhex[r>>uint(s)&0xF])
				}
			}
		}

	}

	return colBuf.String()
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
	colBuf := *bytesBufPool.Get().(*bytes.Buffer)
	defer func() {
		colBuf.Reset()
		bytesBufPool.Put(&colBuf)
	}()

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

// PgStatLiveTuples returns approximate number of rows for the table
func PgStatLiveTuples(pgTx *pgx.Tx, name config.PgTableName) (uint64, error) {
	var rows sql.NullInt64
	err := pgTx.QueryRow("select n_live_tup from pg_stat_all_tables where schemaname = $1 and relname = $2",
		name.SchemaName, name.TableName).Scan(&rows)
	if err != nil || !rows.Valid {
		return 0, err
	}

	return uint64(rows.Int64), nil
}
