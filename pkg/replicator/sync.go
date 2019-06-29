package replicator

import (
	"fmt"
	"log"
	"sort"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

func (r *Replicator) Sync(syncTables []config.PgTableName, async bool) error {
	if len(syncTables) == 0 {
		return nil
	}

	for _, pgTableName := range syncTables {
		r.chTables[pgTableName].StartSync()
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

			log.Printf("all synced!")
		}()
	} else {
		for i := 0; i < r.cfg.SyncWorkers; i++ {
			<-doneCh
		}
		log.Printf("all synced!")
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
		if err := tx.Rollback(); err != nil {
			r.errCh <- err
		}
	}()

	tbl := r.chTables[pgTableName]
	if err := tbl.Sync(tx, snapshotLSN); err != nil {
		return fmt.Errorf("could not sync: %v", err)
	}

	return nil
}

func (r *Replicator) GetSyncTables() ([]config.PgTableName, error) {
	syncTables := make([]config.PgTableName, 0)

	syncNeeded := false
	for tblName := range r.cfg.Tables {
		if !r.persStorage.Has(tblName.KeyName()) {
			syncNeeded = true
			break
		}
	}
	if !syncNeeded {
		return syncTables, nil
	}

	for tblName := range r.cfg.Tables {
		if r.persStorage.Has(tblName.KeyName()) || r.cfg.Tables[tblName].InitSyncSkip {
			continue
		}

		syncTables = append(syncTables, tblName)
	}

	//TODO: adjust hack to get fresh tables first
	sort.SliceStable(syncTables, func(i, j int) bool {
		if len(syncTables[i].TableName) > 6 && len(syncTables[j].TableName) > 6 {
			part1 := syncTables[i].TableName[len(syncTables[i].TableName)-7:]
			part2 := syncTables[j].TableName[len(syncTables[j].TableName)-7:]
			return part1 > part2
		}

		return false
	})

	return syncTables, nil
}

func (r *Replicator) getTxAndLSN(conn *pgx.Conn, pgTableName config.PgTableName) (*pgx.Tx, utils.LSN, error) {
	for attempt := 0; attempt < 10; attempt++ {
		tx, err := r.pgBegin(conn)
		if err != nil {
			log.Printf("could not begin transaction: %v", err)
			r.pgRollback(tx)
			continue
		}

		tmpSlotName := genTempSlotName(pgTableName)
		log.Printf("creating %q temporary logical replication slot for %q pg table (attempt: %d)",
			tmpSlotName, pgTableName.String(), attempt)

		lsn, err := r.pgCreateTempRepSlot(tx, tmpSlotName)
		if err == nil {
			return tx, lsn, nil
		}

		r.pgRollback(tx)
		log.Printf("could not create logical replication slot: %v", err)
	}

	return nil, utils.InvalidLSN, fmt.Errorf("attempts exceeded")
}

// go routine
func (r *Replicator) syncJob(i int, doneCh chan<- struct{}) {
	defer func() {
		doneCh <- struct{}{}
	}()

	for pgTableName := range r.syncJobs {
		log.Printf("sync job %d: starting syncing %q pg table", i, pgTableName.String())
		if err := r.syncTable(pgTableName); err != nil {
			r.errCh <- err
			return
		}

		log.Printf("sync job %d: %q table synced", i, pgTableName.String())
	}
}
