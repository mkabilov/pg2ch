package tableengines

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/pgutils"
)

const (
	syncProgressBatch = 1000000
	gzipFlushCount    = 10000
)

func (t *genericTable) genSyncWrite(p []byte) error {
	row, err := pgutils.DecodeCopyToTuples(p)
	if err != nil {
		return fmt.Errorf("could not parse copy string: %v", err)
	}

	if err := t.bulkUploader.Write(t.convertRow(row)); err != nil {
		return err
	}

	t.syncedRows++

	return nil
}

func (t *genericTable) StartSync() {
	t.Lock()
	defer t.Unlock()

	t.inSync = true
}

func (t *genericTable) genSync(pgTx *pgx.Tx, snapshotLSN utils.LSN, w io.Writer) error {
	if !t.cfg.ChSyncAuxTable.IsEmpty() {
		if err := t.truncateTable(t.cfg.ChSyncAuxTable); err != nil {
			return fmt.Errorf("could not truncate aux table: %v", err)
		}
	}

	t.syncSnapshotLSN = snapshotLSN

	if tblLiveTuples, err := t.pgStatLiveTuples(pgTx); err != nil {
		log.Printf("Could not get approx number of rows in the source table: %v", err)
	} else {
		t.rowsToSync = tblLiveTuples
	}

	log.Printf("Copy from %q postgres table to %q clickhouse table started. ~%d rows to copy, snapshotLSN: %v",
		t.cfg.PgTableName.String(), t.cfg.ChMainTable, t.rowsToSync, snapshotLSN.String())
	if !t.cfg.InitSyncSkipTruncate {
		if err := t.truncateTable(t.cfg.ChMainTable); err != nil {
			return fmt.Errorf("could not truncate main table: %v", err)
		}
	}
	t.syncedRows = 0

	if err := t.bulkUploader.Start(); err != nil {
		return fmt.Errorf("could not init bulkuploader: %v", err)
	}

	loaderErrCh := make(chan error, 1)
	go func(errCh chan error) {
		errCh <- t.bulkUploader.BulkUpload(t.cfg.ChMainTable, t.chUsedColumns)
	}(loaderErrCh)

	t.syncLastBatchTime = time.Now()
	ct, err := pgTx.CopyToWriter(w, fmt.Sprintf("copy (select %s from only %s) to stdout",
		strings.Join(t.pgUsedColumns, ", "), t.cfg.PgTableName.String()))
	if err != nil {
		return fmt.Errorf("could not copy: %v", err)
	}

	if err := t.bulkUploader.Finish(); err != nil {
		return fmt.Errorf("could not finalize bulkuploader: %v", err)
	}

	if pgRows := ct.RowsAffected(); pgRows != int64(t.syncedRows) {
		return fmt.Errorf(
			"number of rows inserted to clickhouse(%v) doesn't match the initial number of rows in pg(%v)",
			t.syncedRows, pgRows)
	}

	log.Printf("%s: copied during sync: %d rows", t.cfg.PgTableName.String(), t.syncedRows)

	if err := <-loaderErrCh; err != nil {
		return fmt.Errorf("could not load to CH: %v", err)
	}
	close(loaderErrCh)

	// post sync
	log.Printf("%s: starting post sync. waiting for current tx to finish", t.cfg.PgTableName.String())
	t.Lock()
	defer t.Unlock()

	if err := t.flushBuffer(); err != nil {
		return fmt.Errorf("could not flush buffer: %v", err)
	}

	log.Printf("%s: delta size: %s", t.cfg.PgTableName.String(), t.deltaSize(snapshotLSN))

	if err := t.chLoader.Exec(
		fmt.Sprintf("INSERT INTO %[1]s(%[2]s) SELECT %[2]s FROM %[3]s WHERE %[4]s > %[5]d ORDER BY %[6]s",
			t.cfg.ChMainTable,
			strings.Join(t.chUsedColumns, ","),
			t.cfg.ChSyncAuxTable,
			t.cfg.LsnColumnName,
			uint64(snapshotLSN),
			t.cfg.BufferTableRowIdColumn)); err != nil {
		return fmt.Errorf("could not merge with sync aux table: %v", err)
	}

	if !t.cfg.ChSyncAuxTable.IsEmpty() {
		if err := t.truncateTable(t.cfg.ChSyncAuxTable); err != nil {
			return fmt.Errorf("could not truncate table: %v", err)
		}
	}

	if err := t.persStorage.Write(t.cfg.PgTableName.KeyName(), snapshotLSN.FormattedBytes()); err != nil {
		return fmt.Errorf("could not save lsn for table %q: %v", t.cfg.PgTableName, err)
	}

	t.inSync = false
	return nil
}

func (t *genericTable) printSyncProgress() {
	if t.syncedRows%syncProgressBatch == 0 {
		var (
			eta  time.Duration
			left uint64
		)
		speed := float64(syncProgressBatch) / time.Since(t.syncLastBatchTime).Seconds()
		if t.rowsToSync >= t.syncedRows {
			left = t.rowsToSync - t.syncedRows
		}

		if t.syncedRows < t.rowsToSync {
			eta = time.Second * time.Duration(left/uint64(speed))
		}

		log.Printf("%s: %d rows copied to %q (ETA: %v left: %v speed: %.0f rows/s)",
			t.cfg.PgTableName.String(), t.syncedRows, t.cfg.ChMainTable, eta.Truncate(time.Second), left, speed)

		t.syncLastBatchTime = time.Now()
	}
}

func (t *genericTable) syncAuxTableColumns() []string {
	return append(t.chUsedColumns, t.cfg.BufferTableRowIdColumn, t.cfg.LsnColumnName)
}

func (t *genericTable) pgStatLiveTuples(pgTx *pgx.Tx) (uint64, error) {
	var rows sql.NullInt64
	err := pgTx.QueryRow("select n_live_tup from pg_stat_all_tables where schemaname = $1 and relname = $2",
		t.cfg.PgTableName.SchemaName,
		t.cfg.PgTableName.TableName).Scan(&rows)
	if err != nil || !rows.Valid {
		return 0, err
	}

	return uint64(rows.Int64), nil
}

func (t *genericTable) deltaSize(lsn utils.LSN) string {
	res, err := t.chLoader.Query(fmt.Sprintf("SELECT count() FROM %s WHERE %s > %d",
		t.cfg.ChSyncAuxTable, t.cfg.LsnColumnName, uint64(lsn)))
	if err != nil {
		log.Fatalf("query error: %v", err)
	}
	return res[0][0]
}
