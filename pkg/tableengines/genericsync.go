package tableengines

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/djherbis/buffer.v1"

	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/pgutils"
)

const (
	syncProgressBatch = 1000000
	gzipFlushCount    = 10000
)

func (t *genericTable) genSyncWrite(p []byte) error {
	row, err := pgutils.DecodeCopyToTuples(t.syncBuf, p)
	if err != nil {
		return fmt.Errorf("could not parse copy string: %v", err)
	}

	if err := t.bulkUploader.Write(t.bufferedConvertRow(t.syncBuf, row)); err != nil {
		return err
	}

	t.syncedRows++

	return nil
}

func (t *genericTable) InitSync() error {
	t.Lock()
	defer t.Unlock()

	if !t.cfg.InitSyncSkipTruncate {
		if err := t.truncateTable(t.cfg.ChMainTable); err != nil {
			return fmt.Errorf("could not truncate main table: %v", err)
		}
	}

	t.inSync = true

	return nil
}

// Sync the table. Saves PostgreSQL snapshot LSN and loads the buffered data
// after this snapshot to the table.
func (t *genericTable) genSync(pgTx *pgx.Tx, snapshotLSN dbtypes.LSN, w io.Writer) error {
	t.syncSnapshotLSN = snapshotLSN

	if tblLiveTuples, err := pgutils.PgStatLiveTuples(pgTx, t.cfg.PgTableName); err != nil {
		t.logger.Warnf("genSync: could not get approximate number of rows: %v", err)
	} else {
		t.liveTuples = tblLiveTuples
	}

	t.logger.Infow("genSync: copy from postgresql to clickhouse table started",
		"postgres", t.cfg.PgTableName.String(),
		"clickhouse", t.cfg.ChMainTable,
		"liveTuples", t.liveTuples,
		"snapshotLSN", snapshotLSN.Dec())
	t.syncedRows = 0

	if err := t.bulkUploader.Init(buffer.New(10 * 1024 * 1024)); err != nil {
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

	t.logger.Infof("copied during the sync: %d rows", t.syncedRows)

	if err := <-loaderErrCh; err != nil {
		return fmt.Errorf("could not load to CH: %v", err)
	}
	close(loaderErrCh)

	return t.postSync()
}

func (t *genericTable) postSync() error {
	t.Lock()
	defer t.Unlock()
	defer func() {
		t.inSync = false
		t.syncBuf = nil
	}()

	if err := t.flushMemBuffer(); err != nil {
		return fmt.Errorf("could not flush buffer: %v", err)
	}

	t.logger.Infof("delta size: %s", t.deltaSize(t.syncSnapshotLSN))

	if err := t.chLoader.Exec(
		fmt.Sprintf("INSERT INTO %[1]s(%[2]s) SELECT %[2]s FROM %[3]s WHERE %[4]s > %[5]d ORDER BY %[6]s",
			t.cfg.ChMainTable,
			strings.Join(t.chUsedColumns, ","),
			t.cfg.ChSyncAuxTable,
			t.cfg.LsnColumnName,
			uint64(t.syncSnapshotLSN),
			t.cfg.BufferTableRowIdColumn)); err != nil {
		return fmt.Errorf("could not merge with sync aux table: %v", err)
	}

	if !t.cfg.ChSyncAuxTable.IsEmpty() {
		if err := t.truncateTable(t.cfg.ChSyncAuxTable); err != nil {
			return fmt.Errorf("could not truncate table: %v", err)
		}
	}

	if err := t.saveLSN(t.syncSnapshotLSN); err != nil {
		return fmt.Errorf("could not save lsn to file: %v", err)
	}

	return nil
}

func (t *genericTable) printSyncProgress() {
	if t.syncedRows%syncProgressBatch == 0 {
		var (
			eta  time.Duration
			left uint64
		)
		speed := float64(syncProgressBatch) / time.Since(t.syncLastBatchTime).Seconds()
		if t.liveTuples >= t.syncedRows {
			left = t.liveTuples - t.syncedRows
		}

		if t.syncedRows < t.liveTuples {
			eta = time.Second * time.Duration(left/uint64(speed))
		}

		t.logger.Infof("%d rows copied to %q (ETA: %v left: %v speed: %.0f rows/s)",
			t.syncedRows, t.cfg.ChMainTable, eta.Truncate(time.Second), left, speed)

		t.syncLastBatchTime = time.Now()
	}
}

func (t *genericTable) syncAuxTableColumns() []string {
	return append(t.chUsedColumns, t.cfg.BufferTableRowIdColumn, t.cfg.LsnColumnName)
}

func (t *genericTable) deltaSize(lsn dbtypes.LSN) string {
	res, err := t.chLoader.Query(fmt.Sprintf("SELECT count(*) FROM %s WHERE %s > %d",
		t.cfg.ChSyncAuxTable, t.cfg.LsnColumnName, uint64(lsn))) //TODO: sql injections
	if err != nil {
		t.logger.Fatalf("query error: %v", err)
	}

	return res[0][0]
}
