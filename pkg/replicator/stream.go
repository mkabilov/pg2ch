package replicator

import (
	"bytes"
	"fmt"
	"log"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

func (r *Replicator) flushTables() error {
	for tblName := range r.tablesToMerge {
		if _, ok := r.inTxTables[tblName]; ok {
			continue
		}
		tbl := r.chTables[tblName]

		if err := tbl.FlushToMainTable(r.finalLSN); err != nil {
			return fmt.Errorf("could not commit %s table: %v", tblName.String(), err)
		}

		delete(r.tablesToMerge, tblName)
	}

	return nil
}

func (r *Replicator) getTable(oid utils.OID) (chTbl clickHouseTable, err error) {
	var lsn utils.LSN

	tblName, ok := r.oidName[oid]
	if !ok {
		return
	}

	chTbl, ok = r.chTables[tblName]
	if !ok {
		return
	}

	if _, ok := r.tablesToMerge[tblName]; !ok {
		r.tablesToMerge[tblName] = struct{}{}
	}

	if tblKey := tblName.KeyName(); r.persStorage.Has(tblKey) {
		if err := lsn.Parse(r.persStorage.ReadString(tblKey)); err != nil {
			log.Fatalf("incorrect lsn stored for %q table: %v", tblName.String(), err)
		}
	}

	if r.finalLSN <= lsn {
		chTbl = nil
	}

	if _, ok := r.inTxTables[tblName]; !ok && chTbl != nil { // TODO: skip adding tables with no buffer table
		r.inTxTables[tblName] = chTbl
		err = chTbl.Begin()
		if err != nil {
			err = fmt.Errorf("could not begin tx for table %q: %v", tblName, err)
		}
	}

	return
}

func (r *Replicator) incrementGeneration() {
	r.generationID++
	if err := r.persStorage.Write("generation_id", []byte(fmt.Sprintf("%v", r.generationID))); err != nil {
		log.Printf("could not save generation id: %v", err)
	}
}

// HandleMessage processes the incoming wal message
func (r *Replicator) HandleMessage(lsn utils.LSN, msg message.Message) error {
	r.tablesToMergeMutex.Lock()
	defer r.tablesToMergeMutex.Unlock()

	switch v := msg.(type) {
	case message.Begin:
		r.inTxMutex.Lock()
		defer r.inTxMutex.Unlock()

		r.inTx = true
		r.finalLSN = v.FinalLSN
		r.curTxMergeIsNeeded = false
		r.isEmptyTx = true
	case message.Commit:
		r.inTxMutex.Lock()
		defer r.inTxMutex.Unlock()

		if r.curTxMergeIsNeeded {
			if err := r.flushTables(); err != nil {
				return fmt.Errorf("could not merge tables: %v", err)
			}
		}
		r.consumer.AdvanceLSN(r.finalLSN)

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
		if chTbl, err := r.getTable(v.OID); err != nil {
			return err
		} else if chTbl == nil {
			break
		}

		if relMsg, ok := r.tblRelMsgs[r.oidName[v.OID]]; ok {
			if bytes.Compare(relMsg.Raw, v.Raw) != 0 {
				log.Fatalln("table name or structure has been changed")
			}
		}
	case message.Insert:
		chTbl, err := r.getTable(v.RelationOID)
		if err != nil {
			return err
		} else if chTbl == nil {
			break
		}

		if mergeIsNeeded, err := chTbl.Insert(r.finalLSN, v.NewRow); err != nil {
			return fmt.Errorf("could not insert: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}

		r.isEmptyTx = false
	case message.Update:
		chTbl, err := r.getTable(v.RelationOID)
		if err != nil {
			return err
		} else if chTbl == nil {
			break
		}

		if mergeIsNeeded, err := chTbl.Update(r.finalLSN, v.OldRow, v.NewRow); err != nil {
			return fmt.Errorf("could not update: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}

		r.isEmptyTx = false
	case message.Delete:
		chTbl, err := r.getTable(v.RelationOID)
		if err != nil {
			return err
		} else if chTbl == nil {
			break
		}

		if mergeIsNeeded, err := chTbl.Delete(r.finalLSN, v.OldRow); err != nil {
			return fmt.Errorf("could not delete: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}

		r.isEmptyTx = false
	case message.Truncate:
		for _, oid := range v.RelationOIDs {
			if chTbl, err := r.getTable(oid); err != nil {
				return err
			} else if chTbl == nil {
				continue
			} else {
				if err := chTbl.Truncate(lsn); err != nil {
					return err
				}
			}
		}

		r.isEmptyTx = false
	}

	return nil
}
