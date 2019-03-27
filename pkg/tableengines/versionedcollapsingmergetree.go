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

/*
:) select * from pgbench_accounts_vercol where aid = 3699;
┌──aid─┬─abalance─┬─sign─┬─ver─┐
│ 3699 │   -21512 │    1 │   0 │
└──────┴──────────┴──────┴─────┘
┌──aid─┬─abalance─┬─sign─┬─────────ver─┐
│ 3699 │   -21512 │   -1 │ 26400473472 │
│ 3699 │   -22678 │    1 │ 26400473472 │
└──────┴──────────┴──────┴─────────────┘

:) select * from pgbench_accounts_vercol final where aid = 3699;
┌──aid─┬─abalance─┬─sign─┬─ver─┐
│ 3699 │   -21512 │    1 │   0 │
└──────┴──────────┴──────┴─────┘

:C
*/

type versionedCollapsingMergeTree struct {
	genericTable

	signColumn string
	verColumn  string
}

// NewVersionedCollapsingMergeTree instantiates versionedCollapsingMergeTree
func NewVersionedCollapsingMergeTree(ctx context.Context, conn *sql.DB, name config.NamespacedName, tblCfg config.Table) *versionedCollapsingMergeTree {
	t := versionedCollapsingMergeTree{
		genericTable: newGenericTable(ctx, conn, name, tblCfg),
		signColumn:   tblCfg.SignColumn,
		verColumn:    tblCfg.VerColumn,
	}

	t.chUsedColumns = append(t.chUsedColumns, t.signColumn, t.verColumn)

	t.flushQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.mainTable, strings.Join(t.chUsedColumns, ", "), t.bufferTable, t.bufferRowIdColumn)}

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *versionedCollapsingMergeTree) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *versionedCollapsingMergeTree) Write(p []byte) (n int, err error) {
	rec, err := utils.DecodeCopy(p)
	if err != nil {
		return 0, err
	}
	n = len(p)

	row, err := t.convertStrings(rec)
	if err != nil {
		return 0, fmt.Errorf("could not parse record: %v", err)
	}
	row = append(row, 1, 0) // append sign and version column values

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
func (t *versionedCollapsingMergeTree) Insert(lsn utils.LSN, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), 1, uint64(lsn)),
	})
}

// Update handles incoming update DML operation
func (t *versionedCollapsingMergeTree) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), -1, uint64(lsn)),
		append(t.convertTuples(new), 1, uint64(lsn)),
	})
}

// Delete handles incoming delete DML operation
func (t *versionedCollapsingMergeTree) Delete(lsn utils.LSN, old message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), -1, uint64(lsn)),
	})
}
