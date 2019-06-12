package replicator

import (
	"fmt"
	"log"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

// TODO: merge with getTable
func (r *Replicator) skipTableMessage(tblName config.PgTableName) bool {
	lsn, ok := r.tableLSN[tblName]
	if !ok {
		return false
	}

	return r.finalLSN <= lsn
}

func (r *Replicator) mergeTables() error {
	for tblName := range r.tablesToMerge {
		if _, ok := r.inTxTables[tblName]; ok {
			continue
		}

		if err := r.chTables[tblName].FlushToMainTable(); err != nil {
			return fmt.Errorf("could not commit %s table: %v", tblName.String(), err)
		}

		delete(r.tablesToMerge, tblName)
		r.tableLSN[tblName] = r.finalLSN
		if err := r.persStorage.Write(tableLSNKeyPrefix+tblName.String(), r.finalLSN.FormattedBytes()); err != nil {
			return fmt.Errorf("could not store lsn for table %s", tblName.String())
		}
	}

	r.advanceLSN()

	return nil
}

func (r *Replicator) getTable(oid utils.OID) (chTbl clickHouseTable, skip bool, startTx bool) {
	tblName, ok := r.oidName[oid]
	if !ok {
		skip = true
		return
	}

	chTbl, ok = r.chTables[tblName]
	if !ok {
		skip = true
		return
	}

	if _, ok := r.tablesToMerge[tblName]; !ok {
		r.tablesToMerge[tblName] = struct{}{}
	}

	lsn, ok := r.tableLSN[tblName]
	if !ok {
		skip = false
	}

	skip = r.finalLSN <= lsn
	if _, ok := r.inTxTables[tblName]; !ok && !skip { // TODO: skip adding tables with no buffer table
		r.inTxTables[tblName] = chTbl
		startTx = true
	}

	return
}

func (r *Replicator) incrementGeneration() {
	r.generationID++
	if err := r.persStorage.Write("generation_id", []byte(fmt.Sprintf("%v", r.generationID))); err != nil {
		log.Printf("could not save generation id: %v", err)
	}
}

func (r *Replicator) advanceLSN() {
	r.consumer.AdvanceLSN(r.finalLSN)
}

// HandleMessage processes the incoming wal message
func (r *Replicator) HandleMessage(lsn utils.LSN, msg message.Message) error {
	r.tablesToMergeMutex.Lock()
	defer r.tablesToMergeMutex.Unlock()

	switch v := msg.(type) {
	case message.Begin:
		r.inTx = true
		r.finalLSN = v.FinalLSN
		r.curTxMergeIsNeeded = false
		r.isEmptyTx = true
	case message.Commit:
		if r.curTxMergeIsNeeded {
			if err := r.mergeTables(); err != nil {
				return fmt.Errorf("could not merge tables: %v", err)
			}
		} else {
			r.advanceLSN()
		}
		if !r.isEmptyTx {
			r.incrementGeneration()

			for _, chTbl := range r.inTxTables {
				if err := chTbl.Commit(); err != nil {
					return fmt.Errorf("could not commit: %v", err)
				}
			}
		}
		r.inTxTables = make(map[config.PgTableName]clickHouseTable)
		r.inTx = false
	case message.Relation:
		chTbl, skip, startTx := r.getTable(v.OID)
		if skip {
			break
		}
		if startTx {
			if err := chTbl.Begin(); err != nil {
				return fmt.Errorf("could not begin tx: %v", err)
			}
		}

		chTbl.SetTupleColumns(v.Columns)
	case message.Insert:
		chTbl, skip, startTx := r.getTable(v.RelationOID)
		if skip {
			break
		}
		if startTx {
			if err := chTbl.Begin(); err != nil {
				return fmt.Errorf("could not begin tx: %v", err)
			}
		}

		if mergeIsNeeded, err := chTbl.Insert(r.finalLSN, v.NewRow); err != nil {
			return fmt.Errorf("could not insert: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
		r.isEmptyTx = false
	case message.Update:
		chTbl, skip, startTx := r.getTable(v.RelationOID)
		if skip {
			break
		}
		if startTx {
			if err := chTbl.Begin(); err != nil {
				return fmt.Errorf("could not begin tx: %v", err)
			}
		}

		if mergeIsNeeded, err := chTbl.Update(r.finalLSN, v.OldRow, v.NewRow); err != nil {
			return fmt.Errorf("could not update: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
		r.isEmptyTx = false
	case message.Delete:
		chTbl, skip, startTx := r.getTable(v.RelationOID)
		if skip {
			break
		}
		if startTx {
			if err := chTbl.Begin(); err != nil {
				return fmt.Errorf("could not begin tx: %v", err)
			}
		}

		if mergeIsNeeded, err := chTbl.Delete(r.finalLSN, v.OldRow); err != nil {
			return fmt.Errorf("could not delete: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
		r.isEmptyTx = false
	case message.Truncate:
		for _, oid := range v.RelationOIDs {
			if chTbl, skip, startTx := r.getTable(oid); skip {
				continue
			} else {
				if startTx {
					if err := chTbl.Begin(); err != nil {
						return fmt.Errorf("could not begin tx: %v", err)
					}
				}

				if err := chTbl.Truncate(lsn); err != nil {
					return err
				}
			}
		}
		r.isEmptyTx = false
	}

	return nil
}
