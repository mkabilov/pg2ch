package tableengines

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/bulkupload"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/pgutils"
)

const (
	syncProgressBatch = 1000000
)

func (t *genericTable) genSyncWrite(p []byte, extras ...[]byte) error {
	if err := bulkupload.DecodeCopyToTuples(t.syncBuf, t.bulkUploader, t.syncPgColumns, t.syncColProps, p); err != nil {
		return fmt.Errorf("could not parse copy string: %v", err)
	}

	if err := t.bulkUploader.WriteByte(columnDelimiter); err != nil {
		return err
	}
	if _, err := t.bulkUploader.Write(t.syncSnapshotLSN.Decimal()); err != nil {
		return err
	}

	if err := t.bulkUploader.WriteByte(columnDelimiter); err != nil {
		return err
	}
	if _, err := t.bulkUploader.Write(t.pgTableName); err != nil {
		return err
	}

	for _, extra := range extras {
		if err := t.bulkUploader.WriteByte(columnDelimiter); err != nil {
			return err
		}

		if _, err := t.bulkUploader.Write(extra); err != nil {
			return err
		}
	}

	if err := t.bulkUploader.WriteByte('\n'); err != nil {
		return err
	}

	t.syncedRows++

	return nil
}

func (t *genericTable) InitSync() error {
	var err error

	t.Lock()
	defer t.Unlock()

	if !t.cfg.InitSyncSkipTruncate {
		if err = t.truncateTable(t.cfg.ChMainTable); err != nil {
			return fmt.Errorf("could not truncate main table: %v", err)
		}
	}

	// if we have no aux table we will create it
	if t.cfg.ChSyncAuxTable.IsEmpty() {
		t.cfg.ChSyncAuxTable.TableName = "aux_" + t.cfg.ChMainTable.TableName
		t.cfg.ChSyncAuxTable.DatabaseName = t.cfg.ChMainTable.DatabaseName
		t.cfg.ChSyncAuxTable.Temporary = true

		if err = t.createAuxTable(t.cfg.ChSyncAuxTable, t.cfg.ChMainTable); err != nil {
			return fmt.Errorf("could not create aux table: %v", err)
		}
	} else {
		if err = t.truncateTable(t.cfg.ChSyncAuxTable); err != nil {
			return fmt.Errorf("could not truncate aux table: %v", err)
		}
	}

	t.inSync = true

	return nil
}

// Sync the table. Saves PostgreSQL snapshot LSN and loads the buffered data
// after this snapshot to the table.
func (t *genericTable) genSync(chUploader bulkupload.BulkUploader, pgTx *pgx.Tx, snapshotLSN dbtypes.LSN, tblWriter io.Writer) error {
	t.syncSnapshotLSN = snapshotLSN
	t.bulkUploader = chUploader
	t.syncedRows = 0
	t.syncLastBatchTime = time.Now()

	startTime := time.Now()
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

	loaderErrCh := make(chan error, 1)
	go func(errCh chan error) {
		errCh <- t.bulkUploader.BulkUpload(t.cfg.ChMainTable, t.chUsedColumns)
	}(loaderErrCh)

	ct, err := pgTx.CopyToWriter(
		tblWriter,
		fmt.Sprintf("copy (select %s from only %s) to stdout",
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

	t.logger.Infof("copied during the sync: %d rows in %v (%0.f rows/sec)",
		t.syncedRows, time.Since(startTime), float64(t.syncedRows)/time.Since(startTime).Seconds())

	if err := <-loaderErrCh; err != nil {
		return fmt.Errorf("could not load to CH: %v", err)
	}
	close(loaderErrCh)
	t.bulkUploader = nil

	return t.loadSyncDeltas()
}

// load changes of the table which occurred during the sync and stored in the aux table
func (t *genericTable) loadSyncDeltas() error {
	t.Lock()
	defer t.Unlock()
	defer func() {
		t.inSync = false
		t.syncBuf = nil
	}()

	if err := t.flushBuffer(); err != nil {
		return fmt.Errorf("could not flush buffer: %v", err)
	}

	t.logger.Infof("delta size: %s", t.deltaSize(t.syncSnapshotLSN))

	if err := utils.Try(t.ctx, 100, time.Second*5, func() error {
		chSql := fmt.Sprintf("INSERT INTO %[1]s(%[2]s) SELECT %[2]s FROM %[3]s WHERE %[4]s > %[5]d and %[7]s = '%[8]s' ORDER BY %[6]s",
			t.cfg.ChMainTable,
			strings.Join(t.chUsedColumns, ","),
			t.cfg.ChSyncAuxTable,
			t.cfg.LsnColumnName, uint64(t.syncSnapshotLSN),
			t.cfg.RowIDColumnName,
			t.cfg.TableNameColumnName, t.pgTableName)
		t.logger.Debugf("executing: %s", chSql)
		if err := t.chLoader.Exec(chSql); err != nil {
			return fmt.Errorf("could not merge with sync aux table: %v", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("failed to merge with sync table: %v", err)
	}

	if !t.cfg.ChSyncAuxTable.IsEmpty() {
		if err := t.dropTablePartition(t.cfg.ChSyncAuxTable, string(t.pgTableName)); err != nil {
			return fmt.Errorf("could not drop partition of the %q table: %v",
				t.cfg.ChSyncAuxTable, err)
		}
	}

	if err := t.saveLSN(t.syncSnapshotLSN); err != nil {
		return fmt.Errorf("could not save lsn to file: %v", err)
	}

	return nil
}

func (t *genericTable) printSyncProgress() {
	if t.syncedRows%syncProgressBatch != 0 {
		return
	}

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

	t.logger.Infof("%d rows copied to %q (ETA: %v left: %v rows speed: %.0f rows/s)",
		t.syncedRows, t.cfg.ChMainTable, eta.Truncate(time.Second), left, speed)

	t.syncLastBatchTime = time.Now()
}

func (t *genericTable) syncAuxTableColumns() []string {
	return append(t.chUsedColumns, t.cfg.RowIDColumnName)
}

func (t *genericTable) deltaSize(lsn dbtypes.LSN) string {
	query := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s > %d and %s = '%s'",
		t.cfg.ChSyncAuxTable,
		t.cfg.LsnColumnName, uint64(lsn),
		t.cfg.TableNameColumnName, t.cfg.PgTableName.NamespacedName(),
	)
	res, err := t.chLoader.Query(query)
	if err != nil {
		t.logger.Fatalw("query error", "err", err, "query", query)
	}

	return res[0][0]
}
