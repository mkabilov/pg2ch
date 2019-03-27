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

type mergeTreeTable struct {
	genericTable
}

// NewMergeTree instantiates mergeTreeTable
func NewMergeTree(ctx context.Context, conn *sql.DB, name config.NamespacedName, tblCfg config.Table) *mergeTreeTable {
	t := mergeTreeTable{
		genericTable: newGenericTable(ctx, conn, name, tblCfg),
	}

	if t.bufferTable == "" {
		return &t
	}

	t.flushQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.mainTable, strings.Join(t.chUsedColumns, ", "), t.bufferTable, t.bufferRowIdColumn)}

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *mergeTreeTable) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *mergeTreeTable) Write(p []byte) (n int, err error) {
	rec, err := utils.DecodeCopy(p)
	if err != nil {
		return 0, err
	}
	n = len(p)

	row, err := t.convertStrings(rec)
	if err != nil {
		return 0, fmt.Errorf("could not parse record: %v", err)
	}

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
func (t *mergeTreeTable) Insert(lsn utils.LSN, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{t.convertTuples(new)})
}

// Update handles incoming update DML operation
func (t *mergeTreeTable) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
	return t.processCommandSet(nil)
}

// Delete handles incoming delete DML operation
func (t *mergeTreeTable) Delete(lsn utils.LSN, old message.Row) (bool, error) {
	return t.processCommandSet(nil)
}
