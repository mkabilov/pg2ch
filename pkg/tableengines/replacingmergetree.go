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

type replacingMergeTree struct {
	genericTable

	verColumn string
}

// NewReplacingMergeTree instantiates replacingMergeTree
func NewReplacingMergeTree(ctx context.Context, conn *sql.DB, name config.NamespacedName, tblCfg config.Table) *replacingMergeTree {
	t := replacingMergeTree{
		genericTable: newGenericTable(ctx, conn, name, tblCfg),
		verColumn:    tblCfg.VerColumn,
	}
	t.chUsedColumns = append(t.chUsedColumns, tblCfg.VerColumn)

	t.flushQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.mainTable, strings.Join(t.chUsedColumns, ", "), t.bufferTable, t.bufferRowIdColumn)}

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *replacingMergeTree) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *replacingMergeTree) Write(p []byte) (n int, err error) {
	rec, err := utils.DecodeCopy(p)
	if err != nil {
		return 0, err
	}
	n = len(p)

	row, err := t.convertStrings(rec)
	if err != nil {
		return 0, fmt.Errorf("could not parse record: %v", err)
	}
	row = append(row, 0) // append version column value

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
func (t *replacingMergeTree) Insert(lsn utils.LSN, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), uint64(lsn)),
	})
}

// Update handles incoming update DML operation
func (t *replacingMergeTree) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), uint64(lsn)),
	})
}

// Delete handles incoming delete DML operation
func (t *replacingMergeTree) Delete(lsn utils.LSN, old message.Row) (bool, error) {
	oldRow := append(t.convertTuples(old), uint64(lsn))

	for id, val := range t.emptyValues {
		oldRow[id] = val
	}

	return t.processCommandSet(commandSet{oldRow})
}
