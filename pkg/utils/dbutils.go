package utils

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx"
)

const (
	// OutputPlugin contains logical decoder plugin name
	OutputPlugin = "pgoutput"

	logicalSlotType = "logical"
	copyNull        = 'N'
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
	} else {
		return `'` + res + `'`
	}
}

// CreateMissingPublication creates missing publication
func CreateMissingPublication(conn *pgx.Conn, publicationName string) error {
	rows, err := conn.Query("select 1 from pg_publication where pubname = $1;", publicationName)
	if err != nil {
		return fmt.Errorf("could not execute query: %v", err)
	}

	for rows.Next() {
		rows.Close()
		return nil
	}
	rows.Close()

	query := fmt.Sprintf("create publication %s for all tables",
		pgx.Identifier{publicationName}.Sanitize())

	if _, err := conn.Exec(query); err != nil {
		return fmt.Errorf("could not create publication: %v", err)
	}
	rows.Close()
	log.Printf("created missing publication: %q", query)

	return nil
}

//GetSlotFlushLSN returns flush LSN of the existing replication slot
func GetSlotFlushLSN(conn *pgx.Conn, slotName, dbName string) (LSN, error) {
	var lsn LSN

	rows, err := conn.Query("select confirmed_flush_lsn, slot_type, database from pg_replication_slots where slot_name = $1;", slotName)
	if err != nil {
		return InvalidLSN, fmt.Errorf("could not execute query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var lsnString, slotType, database string

		if err := rows.Err(); err != nil {
			return InvalidLSN, fmt.Errorf("could not execute query: %v", err)
		}

		if err := rows.Scan(&lsnString, &slotType, &database); err != nil {
			return InvalidLSN, fmt.Errorf("could not scan lsn: %v", err)
		}

		if slotType != logicalSlotType {
			return InvalidLSN, fmt.Errorf("slot %q is not a logical slot", slotName)
		}

		if database != dbName {
			return InvalidLSN, fmt.Errorf("replication slot %q belongs to %q database", slotName, database)
		}

		if err := lsn.Parse(lsnString); err != nil {
			return InvalidLSN, fmt.Errorf("could not parse lsn: %v", err)
		}
	}

	return lsn, nil
}

//CreateSlot creates a slot
func CreateSlot(conn *pgx.Conn, ctx context.Context, slotName string) (LSN, error) {
	var strLSN sql.NullString

	row := conn.QueryRowEx(ctx, "select lsn from pg_create_logical_replication_slot($1, $2)",
		nil, slotName, OutputPlugin)

	if err := row.Scan(&strLSN); err != nil {
		return 0, fmt.Errorf("could not scan: %v", err)
	}
	if !strLSN.Valid {
		return 0, fmt.Errorf("null lsn returned")
	}

	lsn, err := pgx.ParseLSN(strLSN.String)
	if err != nil {
		return 0, fmt.Errorf("could not parse lsn: %v", err)
	}

	return LSN(lsn), nil
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
