package tableengines

import (
	"context"
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
func NewCollapsingMergeTree(ctx context.Context, connUrl, dbName string, tblCfg config.Table, genID *uint64) *collapsingMergeTreeTable {
	t := collapsingMergeTreeTable{
		genericTable: newGenericTable(ctx, connUrl, dbName, tblCfg, genID),
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
	if err := t.genWrite(p); err != nil {
		return 0, err
	}

	if t.cfg.GenerationColumn != "" {
		if err := t.chLoader.BulkAdd([]byte("\t0\t1")); err != nil {
			return 0, err
		}
	} else {
		if err := t.chLoader.BulkAdd([]byte("\t1")); err != nil {
			return 0, err
		}
	}
	if err := t.chLoader.BulkAdd([]byte("\n")); err != nil {
		return 0, err
	}

	t.bufferRowId++

	return len(p), nil
}

// Insert handles incoming insert DML operation
func (t *collapsingMergeTreeTable) Insert(lsn utils.LSN, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), oneStr),
	})
}

// Update handles incoming update DML operation
func (t *collapsingMergeTreeTable) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
	if equal, _ := t.compareRows(old, new); equal {
		return t.processCommandSet(nil)
	}

	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), minusOneStr),
		append(t.convertTuples(new), oneStr),
	})
}

// Delete handles incoming delete DML operation
func (t *collapsingMergeTreeTable) Delete(lsn utils.LSN, old message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), minusOneStr),
	})
}
