package tableengines

import (
	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/bulkupload"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

// collapsingMergeTree is extended with engine-specific properties
type collapsingMergeTreeTable struct {
	genericTable

	signColumn string
}

// NewCollapsingMergeTree instantiates collapsingMergeTreeTable
func NewCollapsingMergeTree(table genericTable, tblCfg *config.Table) *collapsingMergeTreeTable {
	t := collapsingMergeTreeTable{
		genericTable: table,
		signColumn:   tblCfg.SignColumn,
	}
	t.chUsedColumns = append(t.chUsedColumns, tblCfg.SignColumn)

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *collapsingMergeTreeTable) Sync(chUploader bulkupload.BulkUploader, pgTx *pgx.Tx, snapshotLSN dbtypes.LSN) error {
	return t.genSync(chUploader, pgTx, snapshotLSN, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *collapsingMergeTreeTable) Write(p []byte) (int, error) { // sync only
	if err := t.genSyncWrite(p, oneStr); err != nil { // sign
		return 0, err
	}

	t.printSyncProgress()

	return len(p), nil
}

// Insert handles incoming insert DML operation
func (t *collapsingMergeTreeTable) Insert(newRow message.Row) error {
	t.logger.Debugf("insert: %v", newRow)

	return t.writeRow(chTuple{newRow, [][]byte{oneStr}})
}

// Update handles incoming update DML operation
func (t *collapsingMergeTreeTable) Update(oldRow, newRow message.Row) error {
	t.logger.Debugf("update: old: %v new: %v", oldRow, newRow)
	if equal, _ := t.compareRows(oldRow, newRow); equal {
		t.logger.Debugf("update: tuples seem to be identical")
		return t.writeRow()
	}

	if err := t.writeRow(chTuple{oldRow, [][]byte{minusOneStr}}); err != nil {
		return err
	}

	return t.writeRow(chTuple{newRow, [][]byte{oneStr}})
}

// Delete handles incoming delete DML operation
func (t *collapsingMergeTreeTable) Delete(oldRow message.Row) error {
	t.logger.Debugf("delete: %v", oldRow)

	return t.writeRow(chTuple{oldRow, [][]byte{minusOneStr}})
}
