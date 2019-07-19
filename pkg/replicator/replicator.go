package replicator

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/log/zapadapter"
	"github.com/peterbourgon/diskv"
	"go.uber.org/zap"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/consumer"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/tableengines"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

const (
	generationIDKey = "generation_id"
)

type clickHouseTable interface {
	Init() error

	Begin(finalLSN dbtypes.LSN) error
	StartSync() error
	Sync(*pgx.Tx, dbtypes.LSN) error
	Insert(new message.Row) (tblFlushIsNeeded bool, err error)
	Update(old message.Row, new message.Row) (tblFlushIsNeeded bool, err error)
	Delete(old message.Row) (tblFlushIsNeeded bool, err error)
	Truncate() error
	Commit(doFlush bool) error

	FlushToMainTable() error //memory [-> buf table] -> main table; runs outside tx only
}

type Replicator struct {
	wg       *sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	logger   *zap.SugaredLogger
	consumer consumer.Interface
	cfg      config.Config
	errCh    chan error

	chConnString  string
	pgDeltaConn   *pgx.Conn
	pgxConnConfig pgx.ConnConfig
	persStorage   *diskv.Diskv

	chTables map[config.PgTableName]clickHouseTable
	oidName  map[dbtypes.OID]config.PgTableName

	txFinalLSN dbtypes.LSN

	inTxMutex             *sync.Mutex
	inTx                  bool                                   // indicates if we're inside tx
	tablesToFlush         map[config.PgTableName]struct{}        // tables to be merged
	inTxTables            map[config.PgTableName]clickHouseTable // tables inside running tx
	curTxTblFlushIsNeeded bool                                   // if tables in the current transaction are needed to be flushed from buffer tables to main ones
	generationID          uint64                                 // wrap with lock
	isEmptyTx             bool
	syncJobs              chan config.PgTableName
	tblRelMsgs            map[config.PgTableName]message.Relation
}

func New(cfg config.Config) *Replicator {
	zapCfg := zap.Config{
		Development:      true,
		Encoding:         "console",
		EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
		Level:            zap.NewAtomicLevelAt(zap.InfoLevel),
	}
	if cfg.Debug {
		zapCfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

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

		inTxMutex:     &sync.Mutex{},
		tablesToFlush: make(map[config.PgTableName]struct{}),
		inTxTables:    make(map[config.PgTableName]clickHouseTable),
		chConnString:  fmt.Sprintf("http://%s:%d", cfg.ClickHouse.Host, cfg.ClickHouse.Port),
		syncJobs:      make(chan config.PgTableName, cfg.SyncWorkers),
		pgxConnConfig: cfg.Postgres.Merge(pgx.ConnConfig{
			Logger:               zapadapter.NewLogger(logger),
			LogLevel:             pgx.LogLevelWarn,
			RuntimeParams:        map[string]string{"replication": "database", "application_name": config.ApplicationName},
			PreferSimpleProtocol: true}),
		tblRelMsgs: make(map[config.PgTableName]message.Relation, len(cfg.Tables)),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.logger = logger.Sugar()

	if cfg.Debug {
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
	r.persStorage = diskv.New(diskv.Options{
		BasePath:     r.cfg.PersStoragePath,
		CacheSizeMax: 1024 * 1024, // 1MB
	})

	err := r.pgConnect()
	if err != nil {
		return fmt.Errorf("could not connect to postgresql: %v", err)
	}
	defer r.pgDisconnect()

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

	if err := r.readGenerationID(); err != nil {
		return fmt.Errorf("could not get start lsn positions: %v", err)
	}

	if err := r.initTables(); err != nil {
		return fmt.Errorf("could not init tables: %v", err)
	}

	r.consumer = consumer.New(r.ctx, r.logger, r.errCh, r.pgxConnConfig,
		r.cfg.Postgres.ReplicationSlotName, r.cfg.Postgres.PublicationName, startLSN)
	r.txFinalLSN = startLSN

	return nil
}

func (r *Replicator) Run() error {
	defer r.logger.Sync()
	if err := r.Init(); err != nil {
		return err
	}

	r.wg.Add(1)
	go r.logErrCh()

	r.wg.Add(1)
	go r.inactivityTblBufferFlush()

	if r.cfg.RedisBind != "" {
		go r.startRedisServer()
	}

	tablesToSync, err := r.GetSyncTables()
	if err != nil {
		return fmt.Errorf("could not get tables to sync: %v", err)
	}

	if len(tablesToSync) > 0 {
		r.logger.Infof("need to sync %d tables", len(tablesToSync))
		for _, pgTableName := range tablesToSync {
			if err := r.chTables[pgTableName].StartSync(); err != nil {
				return fmt.Errorf("could not start sync for %q table: %v", pgTableName.String(), err)
			}
		}
	}

	if err := r.consumer.Run(r); err != nil {
		return err
	}
	r.logger.Infof("logical replication consumer started")

	if err := r.SyncTables(tablesToSync, true); err != nil {
		return fmt.Errorf("could not sync tables: %v", err)
	}

	r.waitForShutdown()
	r.cancel()
	r.wg.Wait()
	r.consumer.Wait()

	for tblName, tbl := range r.chTables {
		r.logger.Debugw("flushing buffer data", "table", tblName)
		if err := tbl.FlushToMainTable(); err != nil {
			r.logger.Warnw("could not flush buffer data", "table", tblName, "error", err)
		}

		if !r.txFinalLSN.IsValid() {
			continue
		}
	}

	r.logger.Debugf("advancing LSN to %v", r.txFinalLSN)
	r.consumer.AdvanceLSN(r.txFinalLSN)
	if err := r.consumer.SendStatus(); err != nil {
		r.logger.Warnf("could not send status: %v", err)
	}

	return nil
}

func (r *Replicator) newTable(tblName config.PgTableName, tblConfig config.Table) (clickHouseTable, error) {
	defer r.logger.Sync()
	switch tblConfig.Engine {
	case config.ReplacingMergeTree:
		if tblConfig.VerColumn == "" && tblConfig.GenerationColumn == "" {
			return nil, fmt.Errorf("ReplacingMergeTree requires either version or generation column to be set")
		}

		return tableengines.NewReplacingMergeTree(r.ctx, r.logger, r.persStorage, r.chConnString, tblConfig, &r.generationID), nil
	case config.CollapsingMergeTree:
		if tblConfig.SignColumn == "" {
			return nil, fmt.Errorf("CollapsingMergeTree requires sign column to be set")
		}

		return tableengines.NewCollapsingMergeTree(r.ctx, r.logger, r.persStorage, r.chConnString, tblConfig, &r.generationID), nil
	case config.MergeTree:
		return tableengines.NewMergeTree(r.ctx, r.logger, r.persStorage, r.chConnString, tblConfig, &r.generationID), nil
	}

	return nil, fmt.Errorf("%s table engine is not implemented", tblConfig.Engine)
}

func (r *Replicator) initTables() error {
	tx, err := r.pgBegin(r.pgDeltaConn)
	if err != nil {
		return err
	}
	defer r.pgCommit(tx)

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

func (r *Replicator) readGenerationID() error {
	defer r.logger.Sync()
	if !r.persStorage.Has(generationIDKey) {
		return nil
	}

	genID, err := strconv.ParseUint(r.persStorage.ReadString(generationIDKey), 10, 32)
	if err != nil {
		r.logger.Warnf("incorrect value for generation_id in the persistent storage: %v", err)
	}

	r.generationID = genID
	r.logger.Debugf("generation ID: %v", r.generationID)

	return nil
}

func (r *Replicator) inactivityTblBufferFlush() {
	defer r.wg.Done()
	defer r.logger.Sync()

	flushFn := func() {
		r.logger.Debugf("inactivity tbl flush started")
		r.inTxMutex.Lock()
		defer r.inTxMutex.Unlock()
		if r.inTx {
			return
		}

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

func (r *Replicator) logErrCh() {
	defer r.wg.Done()
	defer r.logger.Sync()
	for {
		select {
		case <-r.ctx.Done():
			return
		case err := <-r.errCh:
			r.logger.Fatal(err)
		}
	}
}

func (r *Replicator) waitForShutdown() {
	defer r.logger.Sync()
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
				r.logger.Debugf("got %s signal", sig)
				break loop
			default:
				r.logger.Debugf("unhandled signal: %v", sig)
			}
		}
	}
}
