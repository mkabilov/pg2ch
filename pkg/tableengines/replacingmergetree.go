package tableengines

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

type ReplacingMergeTree struct {
	genericTable

	verColumn string
	row       []interface{}
}

func NewReplacingMergeTree(conn *sql.DB, name string, tblCfg config.Table) *ReplacingMergeTree {
	t := ReplacingMergeTree{
		genericTable: newGenericTable(conn, name, tblCfg),
		verColumn:    tblCfg.VerColumn,
	}
	t.chColumns = append(t.chColumns, tblCfg.VerColumn)
	t.row = make([]interface{}, len(t.chColumns))

	t.mergeQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.mainTable, strings.Join(t.chColumns, ", "), t.bufferTable, t.bufferRowIdColumn)}

	go t.backgroundMerge()

	return &t
}

func (t *ReplacingMergeTree) Write(p []byte) (n int, err error) {
	rec, err := csv.NewReader(bytes.NewReader(p)).Read()
	if err != nil {
		return 0, err
	}

	row := append(t.convertStrings(rec), 0)
	if t.bufferTable != "" {
		row = append(row, t.bufferRowId)
	}

	if err := t.stmntExec(row); err != nil {
		return 0, fmt.Errorf("could not insert: %v", err)
	}
	t.bufferRowId++

	return len(p), nil
}

func (t *ReplacingMergeTree) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

func (t *ReplacingMergeTree) Insert(lsn utils.LSN, new message.Row) error {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), uint64(lsn)),
	})
}

func (t *ReplacingMergeTree) Update(lsn utils.LSN, old, new message.Row) error {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), uint64(lsn)),
	})
}

func (t *ReplacingMergeTree) Delete(lsn utils.LSN, old message.Row) error {
	oldRow := append(t.convertTuples(old), uint64(lsn))

	for id, val := range t.emptyValues {
		oldRow[id] = val
	}

	log.Printf("old row:%v", oldRow)

	return t.processCommandSet(commandSet{oldRow})
}
