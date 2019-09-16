package pgutils

import (
	"bytes"
	"database/sql"
	"sync"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

const (
	// OutputPlugin contains logical decoder plugin name
	OutputPlugin = "pgoutput"
)

var bytesBufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer([]byte{})
	}}

func IstoreToArrays(w utils.Writer, str []byte) error {
	valuesBuf := bytesBufPool.Get().(*bytes.Buffer)
	defer func() {
		valuesBuf.Reset()
		bytesBufPool.Put(valuesBuf)
	}()

	if err := w.WriteByte('['); err != nil {
		return err
	}
	valuesBuf.WriteByte('[')

	first := true
	counter := 0
	isKey := false
	for _, c := range str {
		switch c {
		case '"':
			if counter%2 == 0 {
				isKey = !isKey
				if !first && isKey {
					if err := w.WriteByte(','); err != nil {
						return err
					}

					valuesBuf.WriteByte(',')
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
				if err := w.WriteByte(c); err != nil {
					return err
				}
			} else {
				valuesBuf.WriteByte(c)
			}
		}
	}
	if err := w.WriteByte(']'); err != nil {
		return err
	}
	if err := w.WriteByte('\t'); err != nil {
		return err
	}

	if _, err := w.Write(valuesBuf.Bytes()); err != nil {
		return err
	}
	if err := w.WriteByte(']'); err != nil {
		return err
	}

	return nil
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
