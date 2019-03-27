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
func NewCollapsingMergeTree(ctx context.Context, conn *sql.DB, name config.NamespacedName, tblCfg config.Table) *collapsingMergeTreeTable {
	t := collapsingMergeTreeTable{
		genericTable: newGenericTable(ctx, conn, name, tblCfg),
		signColumn:   tblCfg.SignColumn,
	}
	t.chUsedColumns = append(t.chUsedColumns, tblCfg.SignColumn)

	t.flushQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.mainTable, strings.Join(t.chUsedColumns, ", "), t.bufferTable, t.bufferRowIdColumn)}

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *collapsingMergeTreeTable) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *collapsingMergeTreeTable) Write(p []byte) (n int, err error) {
	rec, err := utils.DecodeCopy(p)
	if err != nil {
		return 0, err
	}
	n = len(p)

	row, err := t.convertStrings(rec) //TODO: move me to genericTable, e.g. fetchCSVRecord
	if err != nil {
		return 0, fmt.Errorf("could not parse record: %v", err)
	}
	row = append(row, 1) // append sign column value

	if t.bufferTable != "" && !t.syncSkipBufferTable {
		row = append(row, t.bufferRowId)
	}

	if err := t.stmntExec(row); err != nil {
		return 0, fmt.Errorf("could not insert: %v", err)
	}
	t.bufferRowId++

	return
}

// Insert handles incoming insert DML operation
func (t *collapsingMergeTreeTable) Insert(lsn utils.LSN, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), 1),
	})
}

// Update handles incoming update DML operation
func (t *collapsingMergeTreeTable) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
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
