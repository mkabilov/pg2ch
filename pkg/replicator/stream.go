package replicator

import (
	"fmt"
	"sort"
	"time"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

func (r *Replicator) tblBuffersFlush() error { // protected by inTxMutex: inactivity merge or on commit
	for tblName := range r.tablesToFlush {
		r.logger.Debugf("processing %s table", tblName)
		select {
		case <-r.ctx.Done():
			return nil
		default:
		}

		if err := r.chTables[tblName].FlushToMainTable(); err != nil {
			return fmt.Errorf("could not commit %s table: %v", tblName.String(), err)
		}

		delete(r.tablesToFlush, tblName)
	}
	r.consumer.AdvanceLSN(r.txFinalLSN) //TODO: wrong?

	return nil
}

func (r *Replicator) getTable(oid dbtypes.OID) (chTbl clickHouseTable, err error) {
	var tblLSN dbtypes.LSN

	tblName, ok := r.oidName[oid]
	if !ok {
		return
	}

	chTbl, ok = r.chTables[tblName]
	if !ok {
		return
	}

	if _, ok := r.tablesToFlush[tblName]; !ok {
		r.tablesToFlush[tblName] = struct{}{}
	}

	if tblKey := tblName.KeyName(); r.persStorage.Has(tblKey) {
		var err error
		tblLSN, err = r.persStorage.ReadLSN(tblKey)

		if err != nil {
			r.logger.Warnf("incorrect lsn stored for %q table: %v", tblName, err)
		}
	}

	if r.txFinalLSN <= tblLSN {
		r.logger.Debugf("tx lsn(%v) <= table lsn(%v)", r.txFinalLSN.Dec(), tblLSN.Dec())
		chTbl = nil
	}

	if _, ok := r.inTxTables[tblName]; !ok && chTbl != nil { // TODO: skip adding tables with no buffer table
		r.inTxTables[tblName] = chTbl
		r.logger.Debugf("table %s was added to inTxTables", tblName)
		err = chTbl.Begin(r.txFinalLSN)
		if err != nil {
			err = fmt.Errorf("could not begin tx for table %q: %v", tblName, err)
		}
	}

	return
}

// Print all replicated tables LSN
func (r *Replicator) PrintTablesLSN() {
	var (
		tables []string
		maxLen int
		lsnMap = make(map[string]string)
	)

	for tblName := range r.chTables {
		var lsn string
		name := tblName.String()

		if len(name) > maxLen {
			maxLen = len(name)
		}

		if tblKey := tblName.KeyName(); r.persStorage.Has(tblKey) {
			tblLSN, err := r.persStorage.ReadLSN(tblKey)

			if err != nil {
				lsn = "INCORRECT"
			} else {
				lsn = tblLSN.String()
			}
		} else {
			lsn = "NO"
		}
		lsnMap[name] = lsn
		tables = append(tables, name)
	}
	sort.Strings(tables)

	// print ordered list of tables
	format := fmt.Sprintf("%%%ds\t%%s\n", maxLen)
	for i := range tables {
		fmt.Printf(format, tables[i], lsnMap[tables[i]])
	}
}

func (r *Replicator) incrementGeneration() {
	r.generationID++
	if err := r.persStorage.WriteUint(generationIDKey, r.generationID); err != nil {
		r.logger.Warnf("could not save generation id: %v", err)
	}
}

func (r *Replicator) inactivityTblBufferFlush() {
	defer r.wg.Done()

	flushFn := func() {
		if r.curState.Load() != stateWorking {
			return
		}
		r.inTxMutex.Lock()
		defer r.inTxMutex.Unlock()

		r.logger.Debugf("inactivity tbl flush started")
		defer r.logger.Debugf("inactivity tbl flush finished")

		if err := r.tblBuffersFlush(); err != nil {
			select {
			case r.errCh <- fmt.Errorf("could not backgound merge tables: %v", err):
			default:
			}
		}
	}

	ticker := time.NewTicker(r.cfg.InactivityFlushTimeout)
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			flushFn()
		}
	}
}

func (r *Replicator) processBegin(finalLSN dbtypes.LSN) error { // TODO: make me lazy: begin transaction on first DML operation
	r.logger.Debugf("begin. trying to acquire lock")
	r.inTxMutex.Lock()
	r.logger.Debugf("begin. lock acquired")
	r.txFinalLSN = finalLSN
	r.curTxTblFlushIsNeeded = false
	r.isEmptyTx = true

	return nil
}

func (r *Replicator) processCommit() error {
	r.logger.Debugf("commit")
	defer r.inTxMutex.Unlock()

	inTxTables := make([]string, 0, len(r.inTxTables))
	for tblName := range r.inTxTables {
		inTxTables = append(inTxTables, tblName.String())
	}

	r.logger.Debugw("commit",
		"isEmptyTx", r.isEmptyTx,
		"inTxTables", inTxTables,
		"flushIsNeeded", r.curTxTblFlushIsNeeded)
	if !r.isEmptyTx {
		r.incrementGeneration()

		for _, chTbl := range r.inTxTables {
			if err := chTbl.Commit(r.curTxTblFlushIsNeeded); err != nil {
				return fmt.Errorf("could not commit: %v", err)
			}
		}
	}
	r.inTxTables = make(map[config.PgTableName]clickHouseTable)
	r.consumer.AdvanceLSN(r.txFinalLSN) // TODO: wrong?

	return nil
}

func (r *Replicator) processRelation(msg *message.Relation) error {
	if chTbl, err := r.getTable(msg.OID); err != nil {
		return err
	} else if chTbl == nil {
		r.logger.Debug("relation message: discarding")
		return nil
	}

	tblName := r.oidName[msg.OID]
	if relMsg, ok := r.tblRelMsgs[tblName]; ok {
		if !relMsg.Equal(msg) {
			r.logger.Fatalf("table or structure of %s table has been changed", tblName)
		}
	}

	return nil
}

func (r *Replicator) processInsert(msg *message.Insert) error {
	chTbl, err := r.getTable(msg.RelationOID)
	if err != nil {
		return err
	} else if chTbl == nil {
		r.logger.Debug("insert message: discarding")
		return nil
	}

	if tblFlushIsNeeded, err := chTbl.Insert(msg.NewRow); err != nil {
		return fmt.Errorf("could not insert: %v", err)
	} else {
		r.curTxTblFlushIsNeeded = r.curTxTblFlushIsNeeded || tblFlushIsNeeded
	}

	r.isEmptyTx = false
	return nil
}

func (r *Replicator) processUpdate(msg *message.Update) error {
	chTbl, err := r.getTable(msg.RelationOID)
	if err != nil {
		return err
	} else if chTbl == nil {
		r.logger.Debug("update message: discarding")
		return nil
	}

	if tblFlushIsNeeded, err := chTbl.Update(msg.OldRow, msg.NewRow); err != nil {
		return fmt.Errorf("could not update: %v", err)
	} else {
		r.curTxTblFlushIsNeeded = r.curTxTblFlushIsNeeded || tblFlushIsNeeded
	}

	r.isEmptyTx = false
	return nil
}

func (r *Replicator) processDelete(msg *message.Delete) error {
	chTbl, err := r.getTable(msg.RelationOID)
	if err != nil {
		return err
	} else if chTbl == nil {
		r.logger.Debug("delete message: discarding")
		return nil
	}

	if tblFlushIsNeeded, err := chTbl.Delete(msg.OldRow); err != nil {
		return fmt.Errorf("could not delete: %v", err)
	} else {
		r.curTxTblFlushIsNeeded = r.curTxTblFlushIsNeeded || tblFlushIsNeeded
	}

	r.isEmptyTx = false
	return nil
}

func (r *Replicator) processTruncate(msg *message.Truncate) error {
	for _, oid := range msg.RelationOIDs {
		if chTbl, err := r.getTable(oid); err != nil {
			return err
		} else if chTbl == nil {
			r.logger.Debug("truncate message: table with oid %v discarding", oid)
			continue
		} else {
			if err := chTbl.Truncate(); err != nil {
				return err
			}
		}
	}

	r.isEmptyTx = false
	return nil
}

// HandleMessage processes the incoming wal message
func (r *Replicator) HandleMessage(lsn dbtypes.LSN, msg message.Message) error {
	r.logger.Debugf("replication message %[1]T: %[1]v", msg)
	if r.txFinalLSN == dbtypes.InvalidLSN {
		if _, ok := msg.(message.Begin); !ok {
			return nil
		}
	}

	switch v := msg.(type) {
	case *message.Begin:
		if r.curState.Load() == stateShuttingDown {
			r.logger.Debugf("shutting down. discarding %T message", msg)
			r.txFinalLSN = dbtypes.InvalidLSN
			return nil
		}

		return r.processBegin(v.FinalLSN)
	case *message.Commit:
		return r.processCommit()
	case *message.Relation:
		return r.processRelation(v)
	case *message.Insert:
		return r.processInsert(v)
	case *message.Update:
		return r.processUpdate(v)
	case *message.Delete:
		return r.processDelete(v)
	case *message.Type:
		r.logger.Debugf("incoming type message: %v", *v)
		return nil
	case *message.Truncate:
		return r.processTruncate(v)
	default:
		return nil
	}
}
