package replicator

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"github.com/peterbourgon/diskv"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/consumer"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/tableengines"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

const (
	applicationName = "pg2ch"
	generationIDKey = "generation_id"
)

type clickHouseTable interface {
	Init() error

	Begin() error
	StartSync()
	Sync(*pgx.Tx, utils.LSN) error
	Insert(lsn utils.LSN, new message.Row) (mergeIsNeeded bool, err error)
	Update(lsn utils.LSN, old message.Row, new message.Row) (mergeIsNeeded bool, err error)
	Delete(lsn utils.LSN, old message.Row) (mergeIsNeeded bool, err error)
	Truncate(lsn utils.LSN) error
	Commit() error

	FlushToMainTable(utils.LSN) error
}

type Replicator struct {
	ctx      context.Context
	cancel   context.CancelFunc
	consumer consumer.Interface
	cfg      config.Config
	errCh    chan error

	chConnString  string
	pgDeltaConn   *pgx.Conn
	pgxConnConfig pgx.ConnConfig
	persStorage   *diskv.Diskv

	chTables map[config.PgTableName]clickHouseTable
	oidName  map[utils.OID]config.PgTableName

	finalLSN utils.LSN

	inTxMutex          *sync.RWMutex
	inTx               bool // indicates if we're inside tx
	tablesToMergeMutex *sync.Mutex
	tablesToMerge      map[config.PgTableName]struct{}        // tables to be merged
	inTxTables         map[config.PgTableName]clickHouseTable // tables inside running tx
	curTxMergeIsNeeded bool                                   // if tables in the current transaction are needed to be merged
	generationID       uint64                                 // wrap with lock
	isEmptyTx          bool
	syncJobs           chan config.PgTableName
	tblRelMsgs         map[config.PgTableName]message.Relation
}

func New(cfg config.Config) *Replicator {
	r := Replicator{
		cfg:      cfg,
		chTables: make(map[config.PgTableName]clickHouseTable),
		oidName:  make(map[utils.OID]config.PgTableName),
		errCh:    make(chan error),

		inTxMutex:          &sync.RWMutex{},
		tablesToMergeMutex: &sync.Mutex{},
		tablesToMerge:      make(map[config.PgTableName]struct{}),
		inTxTables:         make(map[config.PgTableName]clickHouseTable),
		chConnString:       fmt.Sprintf("http://%s:%d", cfg.ClickHouse.Host, cfg.ClickHouse.Port),
		syncJobs:           make(chan config.PgTableName, cfg.SyncWorkers),
		pgxConnConfig: cfg.Postgres.Merge(pgx.ConnConfig{
			RuntimeParams:        map[string]string{"replication": "database", "application_name": applicationName},
			PreferSimpleProtocol: true}),
		tblRelMsgs: make(map[config.PgTableName]message.Relation, len(cfg.Tables)),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return &r
}

func (r *Replicator) newTable(tblName config.PgTableName, tblConfig config.Table) (clickHouseTable, error) {
	switch tblConfig.Engine {
	case config.ReplacingMergeTree:
		if tblConfig.VerColumn == "" && tblConfig.GenerationColumn == "" {
			return nil, fmt.Errorf("ReplacingMergeTree requires either version or generation column to be set")
		}

		return tableengines.NewReplacingMergeTree(r.ctx, r.persStorage, r.chConnString, tblConfig, &r.generationID), nil
	case config.CollapsingMergeTree:
		if tblConfig.SignColumn == "" {
			return nil, fmt.Errorf("CollapsingMergeTree requires sign column to be set")
		}

		return tableengines.NewCollapsingMergeTree(r.ctx, r.persStorage, r.chConnString, tblConfig, &r.generationID), nil
	case config.MergeTree:
		return tableengines.NewMergeTree(r.ctx, r.persStorage, r.chConnString, tblConfig, &r.generationID), nil
	}

	return nil, fmt.Errorf("%s table engine is not implemented", tblConfig.Engine)
}

func (r *Replicator) initTables() error {
	tx, err := r.pgBegin(r.pgDeltaConn)
	if err != nil {
		return err
	}
	defer r.pgRollback(tx)

	for tblName := range r.cfg.Tables {
		tblConfig, err := r.fetchTableConfig(tx, tblName)
		if err != nil {
			return fmt.Errorf("could not get %s table config: %v", tblName.String(), err)
		}

		tbl, err := r.newTable(tblName, tblConfig)
		if err != nil {
			return fmt.Errorf("could not instantiate table: %v", err)
		}

		if err := tbl.Init(); err != nil {
			return fmt.Errorf("could not init %s: %v", tblName.String(), err)
		}

		oid, err := r.fetchTableOID(tblName, tx)
		if err != nil {
			return fmt.Errorf("could not get table oid: %v", err)
		}

		r.oidName[oid] = tblName
		r.chTables[tblName] = tbl
	}

	return nil
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

func (r *Replicator) readGenerationID() error {
	if !r.persStorage.Has(generationIDKey) {
		return nil
	}

	genID, err := strconv.ParseUint(r.persStorage.ReadString(generationIDKey), 10, 32)
	if err != nil {
		log.Printf("incorrect value for generation_id in the pers storage: %v", err)
	}

	r.generationID = genID
	log.Printf("generation_id: %v", r.generationID)

	return nil
}

func (r *Replicator) Init() error {
	r.persStorage = diskv.New(diskv.Options{
		BasePath:     r.cfg.PersStoragePath,
		CacheSizeMax: 1024 * 1024, // 1MB
	})

	if err := r.pgConnect(); err != nil {
		return fmt.Errorf("could not connect to postgresql: %v", err)
	}
	defer r.pgDisconnect()

	if err := r.pgCheck(); err != nil {
		return err
	}

	if err := r.readGenerationID(); err != nil {
		return fmt.Errorf("could not get start lsn positions: %v", err)
	}

	if err := r.initTables(); err != nil {
		return fmt.Errorf("could not init tables: %v", err)
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

func (r *Replicator) Run() error {
	if err := r.Init(); err != nil {
		return err
	}

	r.consumer = consumer.New(r.ctx, r.errCh, r.pgxConnConfig,
		r.cfg.Postgres.ReplicationSlotName, r.cfg.Postgres.PublicationName, r.minLSN())

	go r.logErrCh()
	go r.inactivityMerge()

	if r.cfg.RedisBind != "" {
		go r.redisServer()
	}

	if syncTables, err := r.GetSyncTables(); err != nil {
		return fmt.Errorf("could not get tables to sync: %v", err)
	} else if err := r.Sync(syncTables, true); err != nil {
		return fmt.Errorf("could not sync tables: %v", err)
	}

	if err := r.consumer.Run(r); err != nil {
		return err
	}

	r.waitForShutdown()
	r.cancel()
	log.Printf("waiting for consumer to finish") // debug
	r.consumer.Wait()

	for tblName, tbl := range r.chTables {
		log.Printf("flushing buffer data for %s table", tblName.String()) // debug
		if err := tbl.FlushToMainTable(r.finalLSN); err != nil {
			log.Printf("could not flush %s table: %v", tblName.String(), err)
		}

		if !r.finalLSN.IsValid() {
			continue
		}
	}

	log.Printf("advancing lsn to %v", r.finalLSN) // debug
	r.consumer.AdvanceLSN(r.finalLSN)
	if err := r.consumer.SendStatus(); err != nil {
		log.Printf("could not send status: %v", err)
	}

	return nil
}

func (r *Replicator) inactivityMerge() {
	ticker := time.NewTicker(r.cfg.InactivityFlushTimeout)

	mergeFn := func() {
		r.inTxMutex.RLock()
		defer r.inTxMutex.RUnlock()

		if r.inTx {
			return
		}

		r.tablesToMergeMutex.Lock()
		if err := r.flushTables(); err != nil {
			select {
			case r.errCh <- fmt.Errorf("could not backgound merge tables: %v", err):
			default:
			}
		}
		r.consumer.AdvanceLSN(r.finalLSN)
		r.tablesToMergeMutex.Unlock()
	}

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			mergeFn()
		}
	}
}

func (r *Replicator) logErrCh() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case err := <-r.errCh:
			log.Fatalln(err)
		}
	}
}

func (r *Replicator) waitForShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

loop:
	for {
		select {
		case sig := <-sigs:
			switch sig {
			case syscall.SIGABRT:
				fallthrough
			case syscall.SIGINT:
				fallthrough
			case syscall.SIGQUIT:
				fallthrough
			case syscall.SIGTERM:
				log.Printf("got %s signal", sig)
				break loop
			default:
				log.Printf("unhandled signal: %v", sig)
			}
		}
	}
}
