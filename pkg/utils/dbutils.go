package utils

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// OutputPlugin contains logical decoder plugin name
	OutputPlugin = "pgoutput"

	lowerhex = "0123456789abcdef"
)

var bytesBufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	}}

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
	defer bytesBufPool.Put(&tmpStr)

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
	defer bytesBufPool.Put(&tmpStr)

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
	defer bytesBufPool.Put(&colBuf)

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
