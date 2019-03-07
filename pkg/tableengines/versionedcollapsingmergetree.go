package tableengines

import (
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

type VersionedCollapsingMergeTree struct {
	genericTable

	signColumn string
	verColumn  string
}

func NewVersionedCollapsingMergeTree(conn *sql.DB, name string, tblCfg config.Table) *VersionedCollapsingMergeTree {
	t := VersionedCollapsingMergeTree{
		genericTable: newGenericTable(conn, name, tblCfg),
		signColumn:   tblCfg.SignColumn,
		verColumn:    tblCfg.VerColumn,
	}

	t.chColumns = append(t.chColumns, t.signColumn, t.verColumn)

	t.mergeQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.mainTable, strings.Join(t.chColumns, ", "), t.bufferTable, t.bufferRowIdColumn)}

	return &t
}

func (t *VersionedCollapsingMergeTree) Write(p []byte) (n int, err error) {
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

func (t *VersionedCollapsingMergeTree) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

func (t *VersionedCollapsingMergeTree) Insert(lsn utils.LSN, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), 1, uint64(lsn)),
	})
}

func (t *VersionedCollapsingMergeTree) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), -1, uint64(lsn)),
		append(t.convertTuples(new), 1, uint64(lsn)),
	})
}

func (t *VersionedCollapsingMergeTree) Delete(lsn utils.LSN, old message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), -1, uint64(lsn)),
	})
}
