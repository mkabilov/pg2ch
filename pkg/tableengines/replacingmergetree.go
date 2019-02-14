package tableengines

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"

	"github.com/jackc/pgx"

	"github.com/ikitiki/pg2ch/pkg/config"
	"github.com/ikitiki/pg2ch/pkg/message"
	"github.com/ikitiki/pg2ch/pkg/utils"
)

type ReplacingMergeTree struct {
	genericTable

	verColumn string
}

func NewReplacingMergeTree(conn *sql.DB, name string, tblCfg config.Table) *ReplacingMergeTree {
	t := ReplacingMergeTree{
		genericTable: newGenericTable(conn, name, tblCfg),
		verColumn:    tblCfg.VerColumn,
	}
	t.chColumns = append(t.chColumns, tblCfg.VerColumn)

	return &t
}

func (t *ReplacingMergeTree) Write(p []byte) (n int, err error) {
	rec, err := csv.NewReader(bytes.NewReader(p)).Read()
	if err != nil {
		return 0, err
	}

	if err := t.stmntExec(append(t.convertStrings(rec), 0)); err != nil {
		return 0, fmt.Errorf("could not insert: %v", err)
	}

	return len(p), nil
}

func (t *ReplacingMergeTree) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

func (t *ReplacingMergeTree) Insert(lsn utils.LSN, new message.Row) error {
	return t.bufferCmdSet(commandSet{
		append(t.convertTuples(new), uint64(lsn)),
	})
}

func (t *ReplacingMergeTree) Update(lsn utils.LSN, old, new message.Row) error {
	return t.bufferCmdSet(commandSet{
		append(t.convertTuples(new), uint64(lsn)),
	})
}

func (t *ReplacingMergeTree) Delete(lsn utils.LSN, old message.Row) error {
	return nil
}
