package tableengines

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

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

	t.tblBufferFlushQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.cfg.ChMainTable, strings.Join(t.chUsedColumns, ", "), t.cfg.ChBufferTable, t.cfg.BufferTableRowIdColumn)}

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *collapsingMergeTreeTable) Sync(pgTx *pgx.Tx, snapshotLSN dbtypes.LSN) error {
	return t.genSync(pgTx, snapshotLSN, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *collapsingMergeTreeTable) Write(p []byte) (int, error) { // sync only
	if err := t.genSyncWrite(p); err != nil {
		return 0, err
	}

	if t.cfg.GenerationColumn != "" {
		if err := t.bulkUploader.Write([]byte("\t0\t1")); err != nil { // generation id and sign
			return 0, err
		}
	} else {
		if err := t.bulkUploader.Write([]byte("\t1")); err != nil { // sign
			return 0, err
		}
	}

	if err := t.bulkUploader.Write([]byte("\n")); err != nil {
		return 0, err
	}

	t.printSyncProgress()

	return len(p), nil
}

// Insert handles incoming insert DML operation
func (t *collapsingMergeTreeTable) Insert(newRow message.Row) (bool, error) {
	t.logger.Debugf("insert: %v", newRow)
	return t.processChTuples(chTuples{
		appendField(t.convertRow(t.buf, newRow), oneStr),
	})
}

// Update handles incoming update DML operation
func (t *collapsingMergeTreeTable) Update(oldRow, newRow message.Row) (bool, error) {
	t.logger.Debugf("update: old: %v new: %v", oldRow, newRow)
	if equal, _ := t.compareRows(oldRow, newRow); equal {
		t.logger.Debugf("update: tuples seem to be identical")
		return t.processChTuples(nil)
	}

	return t.processChTuples(chTuples{
		appendField(t.convertRow(t.buf, oldRow), minusOneStr),
		appendField(t.convertRow(t.buf, newRow), oneStr),
	})
}

// Delete handles incoming delete DML operation
func (t *collapsingMergeTreeTable) Delete(oldRow message.Row) (bool, error) {
	t.logger.Debugf("delete: %v", oldRow)
	return t.processChTuples(chTuples{
		appendField(t.convertRow(t.buf, oldRow), minusOneStr),
	})
}
