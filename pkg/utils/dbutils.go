package utils

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/jackc/pgx"
)

const (
	// OutputPlugin contains logical decoder plugin name
	OutputPlugin = "pgoutput"

	logicalSlotType = "logical"
)

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
