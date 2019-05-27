package tableengines

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

type collapsingMergeTreeTable struct {
	genericTable

	signColumn string
}

// NewCollapsingMergeTree instantiates collapsingMergeTreeTable
func NewCollapsingMergeTree(ctx context.Context, conn *sql.DB, tblCfg config.Table, genID *uint64) *collapsingMergeTreeTable {
	t := collapsingMergeTreeTable{
		genericTable: newGenericTable(ctx, conn, tblCfg, genID),
		signColumn:   tblCfg.SignColumn,
	}
	t.chUsedColumns = append(t.chUsedColumns, tblCfg.SignColumn)

	t.flushQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.cfg.ChMainTable, strings.Join(t.chUsedColumns, ", "), t.cfg.ChBufferTable, t.cfg.BufferTableRowIdColumn)}

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *collapsingMergeTreeTable) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *collapsingMergeTreeTable) Write(p []byte) (int, error) {
	var row []interface{}

	row, n, err := t.syncConvertIntoRow(p)
	if err != nil {
		return 0, err
	}

	if t.cfg.GenerationColumn != "" {
		row = append(row, 0) // generationID
	}
	row = append(row, 1) // append sign column value

	return n, t.insertRow(row)
}

// Insert handles incoming insert DML operation
func (t *collapsingMergeTreeTable) Insert(lsn utils.LSN, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), 1),
	})
}

// Update handles incoming update DML operation
func (t *collapsingMergeTreeTable) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
	if equal, _ := t.compareRows(old, new); equal {
		return t.processCommandSet(nil)
	}

	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), -1),
		append(t.convertTuples(new), 1),
	})
}

// Delete handles incoming delete DML operation
func (t *collapsingMergeTreeTable) Delete(lsn utils.LSN, old message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), -1),
	})
}
