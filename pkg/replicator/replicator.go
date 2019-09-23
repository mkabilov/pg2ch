package replicator

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/log/zapadapter"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/consumer"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/tableengines"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/bulkupload"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/chload"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/kvstorage"
)

type clickHouseTable interface {
	Init(lastFinalLSN dbtypes.LSN) error

	Begin(finalLSN dbtypes.LSN) error
	InitSync() error
	Sync(bulkupload.BulkUploader, *pgx.Tx, dbtypes.LSN) error
	Insert(new message.Row) error
	Update(old message.Row, new message.Row) error
	Delete(old message.Row) error
	Truncate() error
	Commit() error

	Flush() error //memory -> main table; runs outside tx only
}

type Replicator struct {
	wg       *sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	logger   *zap.SugaredLogger
	consumer consumer.Interface
	cfg      *config.Config
	errCh    chan error
	stopCh   chan struct{} //used to finish replicator

	pgDeltaConn   *pgx.Conn
	pgxConnConfig pgx.ConnConfig
	persStorage   kvstorage.KVStorage
	pprofHttp     *http.Server

	chTables map[config.PgTableName]clickHouseTable
	oidName  map[dbtypes.OID]config.PgTableName

	txFinalLSN dbtypes.LSN

	inTxMutex             *sync.Mutex
	curState              state
	tablesToFlush         map[config.PgTableName]struct{}        // tables to be merged
	inTxTables            map[config.PgTableName]clickHouseTable // tables inside running tx
	curTxTblFlushIsNeeded bool                                   // if tables in the current transaction are needed to be flushed from buffer tables to main ones
	generationID          uint64                                 // wrap with lock
	isEmptyTx             bool
	syncJobs              chan config.PgTableName
	tblRelMsgs            map[config.PgTableName]message.Relation

	processedMsgCnt     int
	streamLastBatchTime time.Time
}

func New(cfg *config.Config) *Replicator {
	zapCfg := zap.Config{
		Development:      true,
		Encoding:         "console",
		EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
		Level:            zap.NewAtomicLevelAt(cfg.LogLevel),
	}
	zapCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	logger, err := zapCfg.Build()
	if err != nil {
		panic(err)
	}

	r := Replicator{
		wg:       &sync.WaitGroup{},
		cfg:      cfg,
		chTables: make(map[config.PgTableName]clickHouseTable),
		oidName:  make(map[dbtypes.OID]config.PgTableName),
		errCh:    make(chan error),
		stopCh:   make(chan struct{}),

		inTxMutex:     &sync.Mutex{},
		tablesToFlush: make(map[config.PgTableName]struct{}),
		inTxTables:    make(map[config.PgTableName]clickHouseTable),
		syncJobs:      make(chan config.PgTableName, cfg.SyncWorkers),
		pgxConnConfig: cfg.Postgres.Merge(pgx.ConnConfig{
			Logger:   zapadapter.NewLogger(logger),
			LogLevel: pgx.LogLevelWarn,
			RuntimeParams: map[string]string{
				"replication":      "database",
				"application_name": config.ApplicationName},
			PreferSimpleProtocol: true}),
		tblRelMsgs:          make(map[config.PgTableName]message.Relation, len(cfg.Tables)),
		streamLastBatchTime: time.Now(),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.logger = logger.Sugar()
	r.curState.Store(StateInit)

	if cfg.Postgres.Debug {
		r.pgxConnConfig.LogLevel = pgx.LogLevelDebug
	}

	return &r
}

func (r *Replicator) readSlotLSN() (dbtypes.LSN, error) {
	var (
		lsnStr sql.NullString
		lsn    dbtypes.LSN
	)
	err := r.pgDeltaConn.QueryRow("select confirmed_flush_lsn from pg_replication_slots where slot_name = $1",
		r.cfg.Postgres.ReplicationSlotName).Scan(&lsnStr)
	if err != nil {
		return 0, err
	}
	if !lsnStr.Valid {
		return 0, nil
	}
	if err := lsn.Parse(lsnStr.String); err != nil {
		return 0, err
	}

	return lsn, nil
}

func (r *Replicator) Init() error {
	var err error
	r.persStorage, err = kvstorage.New(r.cfg.PersStorageType, r.cfg.PersStoragePath)
	if err != nil {
		return err
	}

	err = r.pgDeltaConnect()
	if err != nil {
		return fmt.Errorf("could not connect to postgresql: %v", err)
	}
	defer r.pgDeltaDisconnect()

	if err := r.pgCheck(); err != nil {
		return err
	}

	startLSN := r.minLSN()
	if !startLSN.IsValid() {
		startLSN, err = r.readSlotLSN()
		if err != nil {
			return fmt.Errorf("could not read confirmed flush lsn of the replication slot: %v", err)
		}
	}

	if err = r.initTables(); err != nil {
		return fmt.Errorf("could not init tables: %v", err)
	}

	r.txFinalLSN = startLSN

	return nil
}

func (r *Replicator) Run() error {
	if err := r.Init(); err != nil {
		return err
	}

	r.wg.Add(1)
	go r.logErrCh()

	r.wg.Add(1)
	go r.inactivityTblBufferFlush()

	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	r.consumer = consumer.New(consumerCtx, r.logger, r.errCh, r.pgxConnConfig,
		r.cfg.Postgres.ReplicationSlotName, r.cfg.Postgres.PublicationName, r.txFinalLSN)

	// we can exit from this function before proper canceling, take care of it
	defer func() {
		if r.consumer != nil {
			consumerCancel()
		}
	}()

	tablesToSync, err := r.GetTablesToSync()
	if err != nil {
		return fmt.Errorf("could not get tables to sync: %v", err)
	}

	if len(tablesToSync) > 0 {
		r.logger.Infof("need to sync %d tables", len(tablesToSync))
		for _, pgTableName := range tablesToSync {
			if err := r.chTables[pgTableName].InitSync(); err != nil {
				return fmt.Errorf("could not start sync for %q table: %v", pgTableName.String(), err)
			}
		}
	}

	if err := r.consumer.Run(r); err != nil {
		return err
	}
	r.curState.Store(StateWorking)

	if r.cfg.RedisBind != "" {
		go r.startRedisServer()
	}

	if r.cfg.PprofBind != "" {
		r.wg.Add(1)
		go r.startPprof()
	}

	if err := r.SyncTables(tablesToSync, true); err != nil {
		return fmt.Errorf("could not sync tables: %v", err)
	}

	r.waitForShutdown()
	if r.curState.Load() != StatePaused {
		r.curState.Store(StateShuttingDown)
		r.inTxMutex.Lock()
	} else {
		r.logger.Debugf("in paused state, no need to wait for tx to finish")
	}

	if r.cfg.PprofBind != "" {
		if err := r.stopPprof(); err != nil {
			r.logger.Warnf("could not stop pprof server: %v", err)
		}
	}

	consumerCancel()
	r.logger.Debugf("waiting for consumer to finish")
	r.consumer.Wait()
	r.consumer = nil

	r.cancel()
	r.logger.Debugf("waiting for go routintes to finish")
	r.wg.Wait()

	for tblName, tbl := range r.chTables {
		r.logger.Debugw("flushing buffer data", "table", tblName)
		if err := tbl.Flush(); err != nil {
			r.logger.Warnw("could not flush buffer data", "table", tblName, "error", err)
		}

		if !r.txFinalLSN.IsValid() {
			continue
		}
	}
	r.logger.Sync()

	r.logger.Debugf("replicator finished its work")
	return nil
}

func (r *Replicator) newTable(tblName config.PgTableName, tblConfig *config.Table) (clickHouseTable, error) {
	chConn := chutils.MakeChConnection(&r.cfg.ClickHouse, false)
	chLoader := chload.New(chConn, r.cfg.GzipCompression)
	baseTable := tableengines.NewGenericTable(r.ctx, r.logger, r.persStorage, chLoader, tblConfig)

	switch tblConfig.Engine {
	case config.ReplacingMergeTree:
		return tableengines.NewReplacingMergeTree(baseTable, tblConfig), nil
	case config.CollapsingMergeTree:
		if tblConfig.SignColumn == "" {
			return nil, fmt.Errorf("CollapsingMergeTree requires sign column to be set")
		}

		return tableengines.NewCollapsingMergeTree(baseTable, tblConfig), nil
	case config.MergeTree:
		return tableengines.NewMergeTree(baseTable, tblConfig), nil
	}

	return nil, fmt.Errorf("%s table engine is not implemented", tblConfig.Engine)
}

func (r *Replicator) initTables() error {
	tx, err := r.pgBegin(r.pgDeltaConn)
	if err != nil {
		return err
	}
	defer r.pgCommit(tx)

	chConn := chutils.MakeChConnection(&r.cfg.ClickHouse, r.cfg.GzipCompression.UseCompression())
	for tblName, tblConfig := range r.cfg.Tables {
		var lsn dbtypes.LSN

		if err := r.fetchTableConfig(tx, chConn, tblName); err != nil {
			return fmt.Errorf("could not get %s table config: %v", tblName.String(), err)
		}

		tbl, err := r.newTable(tblName, tblConfig)
		if err != nil {
			return fmt.Errorf("could not instantiate table: %v", err)
		}

		if r.persStorage.Has(tblName.KeyName()) {
			lsn, err = r.persStorage.ReadLSN(tblName.KeyName())
			if err != nil {
				return fmt.Errorf("could not read lsn for %s table: %v", tblName.String(), err)
			}
		}

		if err := tbl.Init(lsn); err != nil {
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

func (r *Replicator) logErrCh() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case err := <-r.errCh:
			r.logger.Fatal(err)
		}
	}
}

func (r *Replicator) Finish() {
	r.stopCh <- struct{}{}
}

func (r *Replicator) waitForShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

loop:
	for {
		select {
		case <-r.stopCh:
			break loop
		case sig := <-sigs:
			switch sig {
			case syscall.SIGABRT:
				fallthrough
			case syscall.SIGINT:
				fallthrough
			case syscall.SIGQUIT:
				fallthrough
			case syscall.SIGTERM:
				r.logger.Debugf("got %s signal", sig)
				break loop
			default:
				r.logger.Debugf("unhandled signal: %v", sig)
			}
		}
	}
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
