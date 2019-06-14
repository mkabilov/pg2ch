package replicator

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	applicationName   = "pg2ch"
	tableLSNKeyPrefix = "table_lsn_"
	generationIDKey   = "generation_id"
)

type clickHouseTable interface {
	Init() error

	InitSync() error
	Sync(*pgx.Tx, utils.LSN) error

	Begin() error
	Insert(lsn utils.LSN, new message.Row) (mergeIsNeeded bool, err error)
	Update(lsn utils.LSN, old message.Row, new message.Row) (mergeIsNeeded bool, err error)
	Delete(lsn utils.LSN, old message.Row) (mergeIsNeeded bool, err error)
	Truncate(lsn utils.LSN) error
	Commit() error

	SetTupleColumns([]message.Column)
	FlushToMainTable() error
}

type Replicator struct {
	ctx      context.Context
	cancel   context.CancelFunc
	consumer consumer.Interface
	cfg      config.Config
	errCh    chan error

	chConnString string
	chDbName     string

	pgDeltaConn *pgx.Conn

	persStorage *diskv.Diskv

	chTables     map[config.PgTableName]clickHouseTable
	oidName      map[utils.OID]config.PgTableName
	tempSlotName string

	finalLSN utils.LSN
	beginMsg message.Begin

	tableLSNMutex *sync.RWMutex
	tableLSN      map[config.PgTableName]utils.LSN

	inTx               bool // indicates if we're inside tx
	tablesToMergeMutex *sync.Mutex
	tablesToMerge      map[config.PgTableName]struct{}        // tables to be merged
	inTxTables         map[config.PgTableName]clickHouseTable // tables inside running tx
	curTxMergeIsNeeded bool                                   // if tables in the current transaction are needed to be merged
	generationID       uint64                                 // wrap with lock
	isEmptyTx          bool
	syncJobTableName   config.PgTableName
	syncJobs           chan config.PgTableName

	pgxConnConfig pgx.ConnConfig
}

func New(cfg config.Config) *Replicator {
	r := Replicator{
		cfg:      cfg,
		chTables: make(map[config.PgTableName]clickHouseTable),
		oidName:  make(map[utils.OID]config.PgTableName),
		errCh:    make(chan error),

		tableLSNMutex:      &sync.RWMutex{},
		tablesToMergeMutex: &sync.Mutex{},
		tablesToMerge:      make(map[config.PgTableName]struct{}),
		inTxTables:         make(map[config.PgTableName]clickHouseTable),
		tableLSN:           make(map[config.PgTableName]utils.LSN),
		chDbName:           cfg.ClickHouse.Database,
		chConnString:       fmt.Sprintf("http://%s:%d", cfg.ClickHouse.Host, cfg.ClickHouse.Port),
		syncJobs:           make(chan config.PgTableName, cfg.SyncWorkers),
		pgxConnConfig: cfg.Postgres.Merge(pgx.ConnConfig{
			RuntimeParams:        map[string]string{"replication": "database", "application_name": applicationName},
			PreferSimpleProtocol: true}),
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

		return tableengines.NewReplacingMergeTree(r.ctx, r.chConnString, r.chDbName, tblConfig, &r.generationID), nil
	case config.CollapsingMergeTree:
		if tblConfig.SignColumn == "" {
			return nil, fmt.Errorf("CollapsingMergeTree requires sign column to be set")
		}

		return tableengines.NewCollapsingMergeTree(r.ctx, r.chConnString, r.chDbName, tblConfig, &r.generationID), nil
	case config.MergeTree:
		return tableengines.NewMergeTree(r.ctx, r.chConnString, r.chDbName, tblConfig, &r.generationID), nil
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

	tx, err := r.pgBegin(conn)
	if err != nil {
		return err
	}
	defer r.pgRollback(tx)

	tmpSlotName := genTempSlotName(pgTableName)
	log.Printf("creating %q temporary logical replication slot for %q pg table",
		tmpSlotName, pgTableName.String())
	lsn, err := r.pgCreateTempRepSlot(tx, tmpSlotName)
	if err != nil {
		return err
	}

	log.Printf("lsn %v for table %q", uint64(lsn), pgTableName.String())

	tbl := r.chTables[pgTableName]
	if err := tbl.Sync(tx, lsn); err != nil {
		return fmt.Errorf("could not sync: %v", err)
	}

	r.tableLSNMutex.Lock()
	r.tableLSN[pgTableName] = lsn
	r.tableLSNMutex.Unlock()

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

func (r *Replicator) readPersStorage() error {
	r.tableLSNMutex.Lock()
	defer r.tableLSNMutex.Unlock()

	for key := range r.persStorage.Keys(nil) {
		if !strings.HasPrefix(key, tableLSNKeyPrefix) {
			continue
		}
		if !r.persStorage.Has(key) {
			continue
		}
		val, err := r.persStorage.Read(key)
		if err != nil {
			return fmt.Errorf("could not read %v key: %v", err)
		}

		tblName := &config.PgTableName{}
		if err := tblName.Parse(key[len(tableLSNKeyPrefix):]); err != nil {
			return err
		}

		lsn := utils.InvalidLSN
		if err := lsn.Parse(string(val)); err != nil {
			return fmt.Errorf("could not parse lsn %q: %v", string(val), err)
		}

		r.tableLSN[*tblName] = lsn
		log.Printf("consuming changes for table %s starting from %v lsn position", tblName.String(), lsn)
	}

	if !r.persStorage.Has(generationIDKey) {
		return nil
	}

	val, err := r.persStorage.Read(generationIDKey)
	if err != nil {
		return fmt.Errorf("could not read generation id: %v", err)
	}

	genID, err := strconv.ParseUint(string(val), 10, 32)
	if err != nil {
		log.Printf("incorrect value for generation_id in the pers storage: %v", err)
	}

	r.generationID = uint64(genID)
	log.Printf("generation_id: %v", r.generationID)

	return nil
}

func (r *Replicator) Run() error {
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

	if err := r.readPersStorage(); err != nil {
		return fmt.Errorf("could not get start lsn positions: %v", err)
	}

	syncNeeded := false

	r.tableLSNMutex.RLock()
	for tblName := range r.cfg.Tables {
		if _, ok := r.tableLSN[tblName]; !ok {
			syncNeeded = true
			break
		}
	}

	if err := r.initTables(); err != nil {
		return fmt.Errorf("could not init tables: %v", err)
	}

	r.finalLSN = r.minLSN()
	r.consumer = consumer.New(r.ctx, r.errCh, r.cfg.Postgres.ConnConfig,
		r.cfg.Postgres.ReplicationSlotName, r.cfg.Postgres.PublicationName, r.finalLSN)

	syncTables := make([]config.PgTableName, 0)
	if syncNeeded {
		for tblName := range r.cfg.Tables {
			if _, ok := r.tableLSN[tblName]; ok || r.cfg.Tables[tblName].InitSyncSkip {
				continue
			}

			if err := r.chTables[tblName].InitSync(); err != nil {
				return fmt.Errorf("could not init sync %q: %v", tblName, err)
			}
			syncTables = append(syncTables, tblName)
		}
	}
	r.tableLSNMutex.RUnlock()

	if err := r.consumer.Run(r); err != nil {
		return err
	}

	go r.logErrCh()
	go r.inactivityMerge()

	if r.cfg.RedisBind != "" {
		go r.redisServer()
	}

	if syncNeeded {
		doneCh := make(chan struct{}, r.cfg.SyncWorkers)
		for i := 0; i < r.cfg.SyncWorkers; i++ {
			go r.syncJob(i, doneCh)
		}

		for _, tblName := range syncTables {
			r.syncJobs <- tblName
		}
		close(r.syncJobs)

		go func() {
			for i := 0; i < r.cfg.SyncWorkers; i++ {
				<-doneCh
			}

			log.Printf("all synced!")
		}()
	}

	r.waitForShutdown()
	r.cancel()
	r.consumer.Wait()

	for tblName, tbl := range r.chTables {
		if err := tbl.FlushToMainTable(); err != nil {
			log.Printf("could not flush %s table: %v", tblName.String(), err)
		}

		if !r.finalLSN.IsValid() {
			continue
		}

		if err := r.persStorage.Write(tableLSNKeyPrefix+tblName.String(), r.finalLSN.FormattedBytes()); err != nil {
			return fmt.Errorf("could not store lsn for table %s", tblName.String())
		}
	}

	r.consumer.AdvanceLSN(r.finalLSN)

	return nil
}

func (r *Replicator) inactivityMerge() {
	ticker := time.NewTicker(r.cfg.InactivityFlushTimeout)

	mergeFn := func() {
		if r.inTx {
			return
		}

		r.tablesToMergeMutex.Lock()
		if err := r.mergeTables(); err != nil {
			select {
			case r.errCh <- fmt.Errorf("could not backgound merge tables: %v", err):
			default:
			}
		}
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
				break loop
			default:
				log.Printf("unhandled signal: %v", sig)
			}
		}
	}
}
