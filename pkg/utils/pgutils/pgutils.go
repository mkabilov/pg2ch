package pgutils

import (
	"bytes"
	"database/sql"
	"fmt"
	"sync"

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

func IstoreToArrays(buf *bytes.Buffer, str []byte) []byte {
	valuesBuf := bytesBufPool.Get().(*bytes.Buffer)
	defer func() {
		valuesBuf.Reset()
		bytesBufPool.Put(valuesBuf)
	}()

	buf.WriteByte('[')
	valuesBuf.WriteByte('[')

	first := true
	counter := 0
	isKey := false
	for _, c := range str {
		switch c {
		case '"':
			if counter%2 == 0 {
				isKey = !isKey
				if !first {
					if isKey {
						buf.WriteByte(',')
					} else {
						valuesBuf.WriteByte(',')
					}
				}
			}
			counter++
			if counter == 4 {
				first = false
			}
		case '=':
		case '>':
		case ' ':
		case ',':
		default:
			if isKey {
				buf.WriteByte(c)
			} else {
				valuesBuf.WriteByte(c)
			}
		}
	}
	buf.WriteString("]\t")
	buf.Write(valuesBuf.Bytes())
	buf.WriteByte(']')

	return buf.Bytes()
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
func DecodeCopyToTuples(colBuf *bytes.Buffer, in []byte) (message.Row, error) {
	colBuf.Reset()
	result := make(message.Row, 0)

	tupleKind := message.TupleText
	for i, n := 0, len(in); i < n; i++ {
		if in[i] == '\t' {
			t := make([]byte, colBuf.Len())
			copy(t, colBuf.Bytes())

			result = append(result, &message.Tuple{Kind: tupleKind, Value: t})
			tupleKind = message.TupleText
			colBuf.Reset()
			continue
		} else if in[i] == '\n' {
			t := make([]byte, colBuf.Len())
			copy(t, colBuf.Bytes())

			result = append(result, &message.Tuple{Kind: tupleKind, Value: t})
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
