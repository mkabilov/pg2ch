package tableengines

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/jackc/pgx"

	"github.com/ikitiki/pg2ch/pkg/config"
	"github.com/ikitiki/pg2ch/pkg/message"
	"github.com/ikitiki/pg2ch/pkg/utils"
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

	go t.backgroundMerge()

	return &t
}

func (t *VersionedCollapsingMergeTree) Write(p []byte) (n int, err error) {
	rec, err := csv.NewReader(bytes.NewReader(p)).Read()
	if err != nil {
		return 0, err
	}

	row := append(t.convertStrings(rec), 1, 0)
	if t.bufferTable != "" {
		row = append(row, t.bufferRowId)
	}

	if err := t.stmntExec(row); err != nil {
		return 0, fmt.Errorf("could not insert: %v", err)
	}
	t.bufferRowId++

	return len(p), nil
}

func (t *VersionedCollapsingMergeTree) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

func (t *VersionedCollapsingMergeTree) Insert(lsn utils.LSN, new message.Row) error {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), 1, uint64(lsn)),
	})
}

func (t *VersionedCollapsingMergeTree) Update(lsn utils.LSN, old, new message.Row) error {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), -1, uint64(lsn)),
		append(t.convertTuples(new), 1, uint64(lsn)),
	})
}

func (t *VersionedCollapsingMergeTree) Delete(lsn utils.LSN, old message.Row) error {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), -1, uint64(lsn)),
	})
}
