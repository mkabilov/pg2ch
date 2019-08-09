package tableengines

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

type replacingMergeTree struct {
	genericTable

	verColumn string
}

// NewReplacingMergeTree instantiates replacingMergeTree
func NewReplacingMergeTree(table genericTable, tblCfg *config.Table) *replacingMergeTree {
	t := replacingMergeTree{
		genericTable: table,
		verColumn:    tblCfg.VerColumn,
	}
	if tblCfg.VerColumn != "" {
		t.chUsedColumns = append(t.chUsedColumns, tblCfg.VerColumn)
	}
	t.chUsedColumns = append(t.chUsedColumns, tblCfg.IsDeletedColumn)

	t.tblBufferFlushQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.cfg.ChMainTable, strings.Join(t.chUsedColumns, ", "), t.cfg.ChBufferTable, t.cfg.BufferTableRowIdColumn)}

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *replacingMergeTree) Sync(pgTx *pgx.Tx, snapshotLSN dbtypes.LSN) error {
	return t.genSync(pgTx, snapshotLSN, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *replacingMergeTree) Write(p []byte) (int, error) {
	if err := t.genSyncWrite(p); err != nil {
		return 0, err
	}

	suffixes := make([]string, 0)
	if t.cfg.GenerationColumn != "" {
		suffixes = append(suffixes, "0") // generation id
	}
	if t.cfg.VerColumn != "" {
		suffixes = append(suffixes, "0") // version
	}
	suffixes = append(suffixes, "0") // is_deleted

	if len(suffixes) > 0 {
		if err := t.bulkUploader.Write([]byte(strings.Join(suffixes, "\t"))); err != nil {
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
func (t *replacingMergeTree) Insert(new message.Row) (bool, error) {
	if t.cfg.VerColumn != "" {
		return t.processChTuples(chTuples{appendField(t.convertRow(new), t.txFinalLSN.StrBytes(), zeroStr)})
	} else {
		return t.processChTuples(chTuples{appendField(t.convertRow(new), zeroStr)})
	}
}

// Update handles incoming update DML operation
func (t *replacingMergeTree) Update(old, new message.Row) (bool, error) {
	var cmdSet chTuples
	equal, keyChanged := t.compareRows(old, new)
	if equal {
		return t.processChTuples(nil)
	}
	lsnStr := t.txFinalLSN.StrBytes()

	if keyChanged {
		if t.cfg.VerColumn != "" {
			cmdSet = chTuples{
				appendField(t.convertRow(old), lsnStr, oneStr),
				appendField(t.convertRow(new), lsnStr, zeroStr),
			}
		} else {
			cmdSet = chTuples{
				appendField(t.convertRow(old), oneStr),
				appendField(t.convertRow(new), zeroStr),
			}
		}
	} else if t.cfg.VerColumn != "" {
		cmdSet = chTuples{appendField(t.convertRow(new), lsnStr, zeroStr)}
	} else {
		cmdSet = chTuples{appendField(t.convertRow(new), zeroStr)}
	}

	return t.processChTuples(cmdSet)
}

// Delete handles incoming delete DML operation
func (t *replacingMergeTree) Delete(old message.Row) (bool, error) {
	if t.cfg.VerColumn != "" {
		return t.processChTuples(chTuples{appendField(t.convertRow(old), t.txFinalLSN.StrBytes(), zeroStr)})
	} else {
		return t.processChTuples(chTuples{appendField(t.convertRow(old), zeroStr)})
	}
}
