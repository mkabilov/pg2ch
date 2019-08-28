package tableengines

import (
	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

type replacingMergeTree struct {
	genericTable
}

// NewReplacingMergeTree instantiates replacingMergeTree
func NewReplacingMergeTree(table genericTable, tblCfg *config.Table) *replacingMergeTree {
	t := replacingMergeTree{
		genericTable: table,
	}
	t.chUsedColumns = append(t.chUsedColumns, tblCfg.LsnColumnName, tblCfg.IsDeletedColumn)

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *replacingMergeTree) Sync(pgTx *pgx.Tx, snapshotLSN dbtypes.LSN) error {
	return t.genSync(pgTx, snapshotLSN, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *replacingMergeTree) Write(p []byte) (int, error) {
	if err := t.genSyncWrite(p, zeroStr); err != nil {
		return 0, err
	}

	t.printSyncProgress()

	return len(p), nil
}

// Insert handles incoming insert DML operation
func (t *replacingMergeTree) Insert(newRow message.Row) error {
	return t.writeRow(chTuple{newRow, [][]byte{zeroStr}})
}

// Update handles incoming update DML operation
func (t *replacingMergeTree) Update(oldRow, newRow message.Row) error {
	if equal, keyChanged := t.compareRows(oldRow, newRow); equal {
		return t.writeRow()
	} else if keyChanged {
		if err := t.writeRow(chTuple{oldRow, [][]byte{oneStr}}); err != nil { // , is_deleted
			return err
		}
		if err := t.writeRow(chTuple{newRow, [][]byte{zeroStr}}); err != nil { // , is_deleted
			return err
		}
	} else {
		if err := t.writeRow(chTuple{newRow, [][]byte{zeroStr}}); err != nil { // , is_deleted
			return err
		}
	}

	return nil
}

// Delete handles incoming delete DML operation
func (t *replacingMergeTree) Delete(oldRow message.Row) error {
	return t.writeRow(chTuple{oldRow, [][]byte{zeroStr}})
}
