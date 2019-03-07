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

type MergeTreeTable struct {
	genericTable
}

func NewMergeTree(conn *sql.DB, name string, tblCfg config.Table) *MergeTreeTable {
	t := MergeTreeTable{
		genericTable: newGenericTable(conn, name, tblCfg),
	}

	t.mergeQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.mainTable, strings.Join(t.chColumns, ", "), t.bufferTable, t.bufferRowIdColumn)}

	return &t
}

func (t *MergeTreeTable) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

func (t *MergeTreeTable) Write(p []byte) (n int, err error) {
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

func (t *MergeTreeTable) Insert(lsn utils.LSN, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{t.convertTuples(new)})
}

func (t *MergeTreeTable) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
	return t.processCommandSet(nil)
}

func (t *MergeTreeTable) Delete(lsn utils.LSN, old message.Row) (bool, error) {
	return t.processCommandSet(nil)
}
