package replicator

import (
	"fmt"
	"sort"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/pgutils"
)

func (r *Replicator) SyncTables(syncTables []config.PgTableName, async bool) error {
	if len(syncTables) == 0 {
		return nil
	}

	doneCh := make(chan struct{}, r.cfg.SyncWorkers)
	for i := 0; i < r.cfg.SyncWorkers; i++ {
		go r.syncJob(i, doneCh)
	}

	for _, tblName := range syncTables {
		r.syncJobs <- tblName
	}
	close(r.syncJobs)

	if async {
		go func() {
			for i := 0; i < r.cfg.SyncWorkers; i++ {
				<-doneCh
			}

			r.logger.Infof("sync is finished")
		}()
	} else {
		for i := 0; i < r.cfg.SyncWorkers; i++ {
			<-doneCh
		}
		r.logger.Infof("sync is finished")
	}

	return nil
}

func (r *Replicator) syncTable(pgTableName config.PgTableName) error {
	conn, err := pgx.Connect(r.pgxConnConfig)
	if err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			r.errCh <- err
		}
	}()
	connInfo, err := initPostgresql(conn)
	if err != nil {
		return fmt.Errorf("could not fetch conn info: %v", err)
	}
	conn.ConnInfo = connInfo

	tx, snapshotLSN, err := r.getTxAndLSN(conn, pgTableName)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			r.errCh <- err
		}
	}()

	if err := r.chTables[pgTableName].Sync(tx, snapshotLSN); err != nil {
		return fmt.Errorf("could not sync: %v", err)
	}

	return nil
}

func (r *Replicator) GetTablesToSync() ([]config.PgTableName, error) {
	var err error
	syncTables := make([]config.PgTableName, 0)

	if err := r.pgDeltaConnect(); err != nil {
		return nil, fmt.Errorf("could not connect: %v", err)
	}
	defer r.pgDeltaDisconnect()

	tx, err := r.pgBegin(r.pgDeltaConn)
	if err != nil {
		return nil, fmt.Errorf("could not start transaction: %v", err)
	}
	defer r.pgCommit(tx)

	rowsCnt := make(map[config.PgTableName]uint64)
	for tblName := range r.cfg.Tables {
		if r.cfg.Tables[tblName].InitSyncSkip {
			continue
		}

		if r.persStorage.Has(tblName.KeyName()) {
			lsn, _ := r.persStorage.ReadLSN(tblName.KeyName())
			if lsn != dbtypes.InvalidLSN {
				continue
			}
		}

		syncTables = append(syncTables, tblName)

		rowsCnt[tblName], err = pgutils.PgStatLiveTuples(tx, tblName)
		if err != nil {
			return nil, fmt.Errorf("could not get stat live tuples: %v", err)
		}
	}

	if len(syncTables) == 0 {
		return syncTables, nil
	}

	sort.SliceStable(syncTables, func(i, j int) bool {
		return rowsCnt[syncTables[i]] > rowsCnt[syncTables[j]]
	})

	return syncTables, nil
}

func (r *Replicator) getTxAndLSN(conn *pgx.Conn, pgTableName config.PgTableName) (*pgx.Tx, dbtypes.LSN, error) { //TODO: better name: getSnapshot?
	for attempt := 0; attempt < 10; attempt++ {
		select {
		case <-r.ctx.Done():
			return nil, dbtypes.InvalidLSN, fmt.Errorf("context done")
		default:
		}

		tx, err := r.pgBegin(conn)
		if err != nil {
			r.logger.Warnf("could not begin transaction: %v", err)
			r.pgCommit(tx)
			continue
		}

		tmpSlotName := tempSlotName(pgTableName)
		r.logger.Debugf("creating %q temporary logical replication slot for %q pg table (attempt: %d)",
			tmpSlotName, pgTableName, attempt)

		lsn, err := r.pgCreateTempRepSlot(tx, tmpSlotName)
		if err == nil {
			return tx, lsn, nil
		}

		r.pgRollback(tx)
		r.logger.Warnf("could not create logical replication slot: %v", err)
	}

	return nil, dbtypes.InvalidLSN, fmt.Errorf("attempts exceeded")
}

// go routine
func (r *Replicator) syncJob(i int, doneCh chan<- struct{}) {
	defer func() {
		doneCh <- struct{}{}
	}()

	for pgTableName := range r.syncJobs {
		r.logger.Debugf("sync job %d: starting syncing %q pg table", i, pgTableName)
		if err := r.syncTable(pgTableName); err != nil {
			r.errCh <- err
			return
		}

		r.logger.Debugf("sync job %d: %q table synced", i, pgTableName)
	}
}
