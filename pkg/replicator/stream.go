package replicator

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

const (
	stateIdle uint32 = iota
	statePaused
	statePausing
	stateInactivityFlush
)

func (r *Replicator) tblBuffersFlush() error { // protected by inTxMutex: inactivity merge or on commit
	defer r.logger.Sync()
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
	defer r.logger.Sync()
	var lsn dbtypes.LSN

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
		if err := lsn.Parse(r.persStorage.ReadString(tblKey)); err != nil {
			r.logger.Warnf("incorrect lsn stored for %q table: %v", tblName, err)
		}
	}

	if r.txFinalLSN <= lsn {
		r.logger.Debugf("tx lsn(%v) <= table lsn(%v)", r.txFinalLSN.Dec(), lsn.Dec())
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

func (r *Replicator) incrementGeneration() {
	defer r.logger.Sync()
	r.generationID++
	if err := r.persStorage.WriteString(generationIDKey, fmt.Sprintf("%v", r.generationID)); err != nil {
		r.logger.Warnf("could not save generation id: %v", err)
	}
}

func (r *Replicator) inactivityTblBufferFlush() {
	defer r.wg.Done()
	defer r.logger.Sync()

	flushFn := func() {
		if atomic.LoadUint32(&r.state) != stateIdle {
			return
		}
		r.inTxMutex.Lock()
		defer r.inTxMutex.Unlock()

		r.logger.Debugf("inactivity tbl flush started")
		if !atomic.CompareAndSwapUint32(&r.state, stateIdle, stateInactivityFlush) {
			return
		}

		if err := r.tblBuffersFlush(); err != nil {
			select {
			case r.errCh <- fmt.Errorf("could not backgound merge tables: %v", err):
			default:
			}
		}

		atomic.StoreUint32(&r.state, stateIdle)
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
	r.logger.Debugf("begin: trying to acquire inTxMutex lock")
	r.inTxMutex.Lock()
	r.logger.Debugf("begin: inTxMutex lock acquired")
	defer r.inTxMutex.Unlock()

	r.txFinalLSN = finalLSN
	r.curTxTblFlushIsNeeded = false
	r.isEmptyTx = true

	return nil
}

func (r *Replicator) processCommit() error {
	r.logger.Debugf("commit: trying to acquire inTxMutex lock")
	r.inTxMutex.Lock()
	r.logger.Debugf("commit: inTxMutex lock acquired")
	select {
	case <-r.ctx.Done():
		r.logger.Debugf("commit: got context cancel")
		defer r.wg.Done()
	default:
	}

	defer atomic.StoreUint32(&r.state, stateIdle)
	defer r.inTxMutex.Unlock()
	defer r.logger.Sync()

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

func (r *Replicator) processRelation(msg message.Relation) error {
	defer r.logger.Sync()
	if chTbl, err := r.getTable(msg.OID); err != nil {
		return err
	} else if chTbl == nil {
		r.logger.Debug("relation message: discarding")
		return nil
	}

	tblName := r.oidName[msg.OID]
	if relMsg, ok := r.tblRelMsgs[tblName]; ok {
		if bytes.Compare(relMsg.Raw, msg.Raw) != 0 {
			r.logger.Fatalf("table or structure of %s table has been changed", tblName)
		}
	}

	return nil
}

func (r *Replicator) processInsert(msg message.Insert) error {
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

func (r *Replicator) processUpdate(msg message.Update) error {
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

func (r *Replicator) processDelete(msg message.Delete) error {
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

func (r *Replicator) processTruncate(msg message.Truncate) error {
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
	defer r.logger.Sync()

	r.logger.Debugf("replication message %[1]T: %[1]v", msg)
	switch v := msg.(type) {
	case message.Begin:
		return r.processBegin(v.FinalLSN)
	case message.Commit:
		return r.processCommit()
	case message.Relation:
		return r.processRelation(v)
	case message.Insert:
		return r.processInsert(v)
	case message.Update:
		return r.processUpdate(v)
	case message.Delete:
		return r.processDelete(v)
	case message.Truncate:
		return r.processTruncate(v)
	default:
		return fmt.Errorf("unknown message type %T", v)
	}
}
