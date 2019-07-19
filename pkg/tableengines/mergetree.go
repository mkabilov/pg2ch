package tableengines

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx"
	"github.com/peterbourgon/diskv"
	"go.uber.org/zap"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

type mergeTreeTable struct {
	genericTable
}

// NewMergeTree instantiates mergeTreeTable
func NewMergeTree(ctx context.Context, logger *zap.SugaredLogger, persStorage *diskv.Diskv, connUrl string, tblCfg config.Table, genID *uint64) *mergeTreeTable {
	t := mergeTreeTable{
		genericTable: newGenericTable(ctx, logger, persStorage, connUrl, tblCfg, genID),
	}

	if t.cfg.ChBufferTable.IsEmpty() {
		return &t
	}

	t.tblBufferFlushQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.cfg.ChMainTable, strings.Join(t.chUsedColumns, ", "), t.cfg.ChBufferTable, t.cfg.BufferTableRowIdColumn)}

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *mergeTreeTable) Sync(pgTx *pgx.Tx, snapshotLSN dbtypes.LSN) error {
	return t.genSync(pgTx, snapshotLSN, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *mergeTreeTable) Write(p []byte) (int, error) {
	if err := t.genSyncWrite(p); err != nil {
		return 0, err
	}

	if t.cfg.GenerationColumn != "" {
		if err := t.bulkUploader.Write([]byte("\t0")); err != nil { // generation id
			return 0, err
		}
	}
	if err := t.bulkUploader.Write([]byte("\n")); err != nil {
		return 0, err
	}

	t.printSyncProgress()

	return len(p), nil
}

// Insert handles incoming insert DML operation
func (t *mergeTreeTable) Insert(new message.Row) (bool, error) {
	return t.processChTuples(chTuples{t.convertRow(new)})
}

// Update handles incoming update DML operation
func (t *mergeTreeTable) Update(old, new message.Row) (bool, error) {
	return t.processChTuples(nil)
}

// Delete handles incoming delete DML operation
func (t *mergeTreeTable) Delete(old message.Row) (bool, error) {
	return t.processChTuples(nil)
}
