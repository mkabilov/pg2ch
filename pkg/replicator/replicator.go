package replicator

import (
	"context"
	"database/sql"
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
	"github.com/kshvakov/clickhouse"
	"github.com/peterbourgon/diskv"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/consumer"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/tableengines"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/tableinfo"
)

const (
	applicationName   = "pg2ch"
	tableLSNKeyPrefix = "table_lsn_"
	generationIDKey   = "generation_id"
)

type clickHouseTable interface {
	Insert(lsn utils.LSN, new message.Row) (mergeIsNeeded bool, err error)
	Update(lsn utils.LSN, old message.Row, new message.Row) (mergeIsNeeded bool, err error)
	Delete(lsn utils.LSN, old message.Row) (mergeIsNeeded bool, err error)
	SetTupleColumns([]message.Column)
	Truncate() error
	Sync(*pgx.Tx) error
	Init() error
	FlushToMainTable() error
}

type Replicator struct {
	ctx      context.Context
	cancel   context.CancelFunc
	consumer consumer.Interface
	cfg      config.Config
	errCh    chan error

	pgConn *pgx.Conn
	chConn *sql.DB

	persStorage *diskv.Diskv

	chTables     map[config.PgTableName]clickHouseTable
	oidName      map[utils.OID]config.PgTableName
	tempSlotName string

	finalLSN utils.LSN
	tableLSN map[config.PgTableName]utils.LSN

	inTx               bool // indicates if we're inside tx
	tablesToMergeMutex *sync.Mutex
	tablesToMerge      map[config.PgTableName]struct{} // tables to be merged
	inTxTables         map[config.PgTableName]struct{} // tables inside running tx
	curTxMergeIsNeeded bool                            // if tables in the current transaction are needed to be merged
	generationID       uint64
	isEmptyTx          bool
}

func New(cfg config.Config) *Replicator {
	r := Replicator{
		cfg:      cfg,
		chTables: make(map[config.PgTableName]clickHouseTable),
		oidName:  make(map[utils.OID]config.PgTableName),
		errCh:    make(chan error),

		tablesToMergeMutex: &sync.Mutex{},
		tablesToMerge:      make(map[config.PgTableName]struct{}),
		inTxTables:         make(map[config.PgTableName]struct{}),
		tableLSN:           make(map[config.PgTableName]utils.LSN),
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

		return tableengines.NewReplacingMergeTree(r.ctx, r.chConn, tblConfig, &r.generationID), nil
	case config.CollapsingMergeTree:
		if tblConfig.SignColumn == "" {
			return nil, fmt.Errorf("CollapsingMergeTree requires sign column to be set")
		}

		return tableengines.NewCollapsingMergeTree(r.ctx, r.chConn, tblConfig, &r.generationID), nil
	case config.MergeTree:
		return tableengines.NewMergeTree(r.ctx, r.chConn, tblConfig, &r.generationID), nil
	}

	return nil, fmt.Errorf("%s table engine is not implemented", tblConfig.Engine)
}

func (r *Replicator) checkPgSlotAndPub(tx *pgx.Tx) error {
	var slotExists, pubExists bool

	err := tx.QueryRow("select "+
		"exists(select 1 from pg_replication_slots where slot_name = $1) as slot_exists, "+
		"exists(select 1 from pg_publication where pubname = $2) as pub_exists",
		r.cfg.Postgres.ReplicationSlotName, r.cfg.Postgres.PublicationName).Scan(&slotExists, &pubExists)

	if err != nil {
		return fmt.Errorf("could not query: %v", err)
	}

	errMsg := ""

	if !slotExists {
		errMsg += fmt.Sprintf("slot %q does not exist", r.cfg.Postgres.ReplicationSlotName)
	}

	if !pubExists {
		if errMsg != "" {
			errMsg += " and "
		}
		errMsg += fmt.Sprintf("publication %q does not exist", r.cfg.Postgres.PublicationName)
	}

	if errMsg != "" {
		return fmt.Errorf(errMsg)
	}

	return nil
}

func (r *Replicator) initAndSyncTables() error {
	for tblName := range r.cfg.Tables {
		var (
			lsn utils.LSN
			err error
		)

		tx, err := r.pgBegin()
		if err != nil {
			return err
		}

		if _, ok := r.tableLSN[tblName]; !ok {
			lsn, err = r.pgCreateTempRepSlot(tx, tblName) // create temp repl slot must the first command in the tx
			if err != nil {
				return fmt.Errorf("could not create temporary replication slot: %v", err)
			}
		}

		tblConfig, err := r.fetchTableConfig(tx, tblName)
		if err != nil {
			return fmt.Errorf("could not get %s table config: %v", tblName.String(), err)
		}
		tblConfig.PgTableName = tblName

		tbl, err := r.newTable(tblName, tblConfig)
		if err != nil {
			return fmt.Errorf("could not instantiate table: %v", err)
		}

		if err := tbl.Init(); err != nil {
			return fmt.Errorf("could not init %s: %v", tblName.String(), err)
		}

		r.chTables[tblName] = tbl

		if _, ok := r.tableLSN[tblName]; ok {
			if err := tx.Commit(); err != nil {
				return err
			}

			continue
		}

		if err := tbl.Sync(tx); err != nil {
			return fmt.Errorf("could not sync %s: %v", tblName.String(), err)
		}

		r.tableLSN[tblName] = lsn
		if err := r.persStorage.Write(tableLSNKeyPrefix+tblName.String(), lsn.Bytes()); err != nil {
			return fmt.Errorf("could not store lsn for table %s", tblName.String())
		}

		if err := r.pgDropRepSlot(tx); err != nil {
			return fmt.Errorf("could not drop replication slot: %v", err)
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}
	r.incrementGeneration()

	return nil
}

func (r *Replicator) pgBegin() (*pgx.Tx, error) {
	tx, err := r.pgConn.BeginEx(r.ctx, &pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("could not start pg transaction: %v", err)
	}

	return tx, nil
}

func (r *Replicator) pgCommit(tx *pgx.Tx) error {
	return tx.Commit()
}

func (r *Replicator) readPersStorage() error {
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

func (r *Replicator) initTables(tx *pgx.Tx) error {
	for tblName := range r.cfg.Tables {
		tblConfig, err := r.fetchTableConfig(tx, tblName)
		if err != nil {
			return fmt.Errorf("could not get %s table config: %v", tblName.String(), err)
		}
		tblConfig.PgTableName = tblName

		tbl, err := r.newTable(tblName, tblConfig)
		if err != nil {
			return fmt.Errorf("could not instantiate table: %v", err)
		}

		if err := tbl.Init(); err != nil {
			return fmt.Errorf("could not init %s: %v", tblName.String(), err)
		}

		r.chTables[tblName] = tbl
	}

	return nil
}

func (r *Replicator) minLSN() utils.LSN {
	result := utils.InvalidLSN
	if len(r.tableLSN) == 0 {
		return result
	}

	for _, lsn := range r.tableLSN {
		if !result.IsValid() || lsn < result {
			result = lsn
		}
	}

	return result
}

func (r *Replicator) pgCheck() error {
	tx, err := r.pgBegin()
	if err != nil {
		return fmt.Errorf("could not begin: %v", err)
	}

	if err := r.checkPgSlotAndPub(tx); err != nil {
		return err
	}

	if err := r.pgCommit(tx); err != nil {
		return fmt.Errorf("could not commit: %v", err)
	}

	return nil
}

func (r *Replicator) Run() error {
	var (
		tx  *pgx.Tx
		err error
	)

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

	if err := r.chConnect(); err != nil {
		return fmt.Errorf("could not connect to clickhouse: %v", err)
	}
	defer r.chDisconnect()

	if err := r.readPersStorage(); err != nil {
		return fmt.Errorf("could not get start lsn positions: %v", err)
	}

	syncNeeded := false
	for tblName := range r.cfg.Tables {
		if _, ok := r.tableLSN[tblName]; !ok {
			syncNeeded = true
			break
		}
	}

	if syncNeeded {
		// in case of init sync, the replication slot must be created, which must be called before any query
		if err := r.initAndSyncTables(); err != nil {
			return fmt.Errorf("could not sync tables: %v", err)
		}

		tx, err = r.pgBegin()
		if err != nil {
			return err
		}
	} else {
		tx, err = r.pgBegin()
		if err != nil {
			return err
		}

		if err := r.initTables(tx); err != nil {
			return fmt.Errorf("could not init tables: %v", err)
		}
	}

	if err := r.fetchPgTablesInfo(tx); err != nil {
		return fmt.Errorf("table check failed: %v", err)
	}

	if err := r.pgCommit(tx); err != nil {
		return err
	}

	r.finalLSN = r.minLSN()
	r.consumer = consumer.New(r.ctx, r.errCh, r.cfg.Postgres.ConnConfig,
		r.cfg.Postgres.ReplicationSlotName, r.cfg.Postgres.PublicationName, r.finalLSN)

	if err := r.consumer.Run(r); err != nil {
		return err
	}

	go r.logErrCh()
	go r.inactivityMerge()

	if r.cfg.RedisBind != "" {
		go r.redisServer()
	}

	r.waitForShutdown()
	r.cancel()
	r.consumer.Wait()

	for tblName, tbl := range r.chTables {
		if err := tbl.FlushToMainTable(); err != nil {
			log.Printf("could not flush %s table: %v", tblName.String(), err)
		}

		if err := r.persStorage.Write(tableLSNKeyPrefix+tblName.String(), r.finalLSN.Bytes()); err != nil {
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
			log.Println(err)
		}
	}
}

func (r *Replicator) fetchPgTablesInfo(tx *pgx.Tx) error {
	rows, err := tx.Query(`
			select c.oid,
				   n.nspname,
				   c.relname,
				   c.relreplident
			from pg_class c
				   join pg_namespace n on n.oid = c.relnamespace
      			   join pg_publication_tables pub on (c.relname = pub.tablename and n.nspname = pub.schemaname)
			where
				c.relkind = 'r'
				and pub.pubname = $1`, r.cfg.Postgres.PublicationName)

	if err != nil {
		return fmt.Errorf("could not exec: %v", err)
	}

	for rows.Next() {
		var (
			oid                   utils.OID
			schemaName, tableName string
			replicaIdentity       message.ReplicaIdentity
		)

		if err := rows.Scan(&oid, &schemaName, &tableName, &replicaIdentity); err != nil {
			return fmt.Errorf("could not scan: %v", err)
		}

		fqName := config.PgTableName{SchemaName: schemaName, TableName: tableName}

		if _, ok := r.cfg.Tables[fqName]; ok && replicaIdentity != message.ReplicaIdentityFull {
			return fmt.Errorf("table %s must have FULL replica identity(currently it is %q)", tableName, replicaIdentity)
		}

		r.oidName[oid] = fqName
	}

	return nil
}

func (r *Replicator) chConnect() error {
	var err error

	r.chConn, err = sql.Open("clickhouse", r.cfg.ClickHouse.ConnectionString())
	if err != nil {
		log.Fatal(err)
	}
	if err := r.chConn.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return fmt.Errorf("[%d] %s %s", exception.Code, exception.Message, exception.StackTrace)
		}

		return fmt.Errorf("could not ping: %v", err)
	}

	return nil
}

func (r *Replicator) chDisconnect() {
	if err := r.chConn.Close(); err != nil {
		log.Printf("could not close connection to clickhouse: %v", err)
	}
}

func (r *Replicator) pgConnect() error {
	var err error

	r.pgConn, err = pgx.Connect(r.cfg.Postgres.Merge(pgx.ConnConfig{
		RuntimeParams:        map[string]string{"replication": "database", "application_name": applicationName},
		PreferSimpleProtocol: true}))
	if err != nil {
		return fmt.Errorf("could not rep connect to pg: %v", err)
	}

	connInfo, err := initPostgresql(r.pgConn)
	if err != nil {
		return fmt.Errorf("could not fetch conn info: %v", err)
	}
	r.pgConn.ConnInfo = connInfo

	return nil
}

func (r *Replicator) pgDisconnect() {
	if err := r.pgConn.Close(); err != nil {
		log.Printf("could not close connection to postgresql: %v", err)
	}
}

func (r *Replicator) pgDropRepSlot(tx *pgx.Tx) error {
	_, err := tx.Exec(fmt.Sprintf("DROP_REPLICATION_SLOT %s", r.tempSlotName))

	return err
}

func (r *Replicator) pgCreateTempRepSlot(tx *pgx.Tx, tblName config.PgTableName) (utils.LSN, error) {
	var (
		snapshotLSN, snapshotName, plugin sql.NullString
		lsn                               utils.LSN
	)

	row := tx.QueryRow(fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s USE_SNAPSHOT",
		fmt.Sprintf("ch_tmp_%s_%s", tblName.SchemaName, tblName.TableName), utils.OutputPlugin))

	if err := row.Scan(&r.tempSlotName, &snapshotLSN, &snapshotName, &plugin); err != nil {
		return utils.InvalidLSN, fmt.Errorf("could not scan: %v", err)
	}

	if err := lsn.Parse(snapshotLSN.String); err != nil {
		return utils.InvalidLSN, fmt.Errorf("could not parse LSN: %v", err)
	}

	return lsn, nil
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

// TODO: merge with getTable
func (r *Replicator) skipTableMessage(tblName config.PgTableName) bool {
	lsn, ok := r.tableLSN[tblName]
	if !ok {
		return false
	}

	return r.finalLSN <= lsn
}

func (r *Replicator) getTable(oid utils.OID) (config.PgTableName, clickHouseTable) {
	tblName, ok := r.oidName[oid]
	if !ok {
		return config.PgTableName{}, nil
	}

	chTbl, ok := r.chTables[tblName]
	if !ok {
		return config.PgTableName{}, nil
	}

	// TODO: skip adding tables with no buffer table
	if _, ok := r.inTxTables[tblName]; !ok {
		r.inTxTables[tblName] = struct{}{}
	}

	if _, ok := r.tablesToMerge[tblName]; !ok {
		r.tablesToMerge[tblName] = struct{}{}
	}

	return tblName, chTbl
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
		if err := r.persStorage.Write(tableLSNKeyPrefix+tblName.String(), r.finalLSN.Bytes()); err != nil {
			return fmt.Errorf("could not store lsn for table %s", tblName.String())
		}
	}

	r.advanceLSN()

	return nil
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
		}
		r.inTxTables = make(map[config.PgTableName]struct{})
		r.inTx = false
	case message.Relation:
		_, chTbl := r.getTable(v.OID)
		if chTbl == nil {
			break
		}

		chTbl.SetTupleColumns(v.Columns)
	case message.Insert:
		tblName, chTbl := r.getTable(v.RelationOID)
		if chTbl == nil || r.skipTableMessage(tblName) {
			break
		}

		if mergeIsNeeded, err := chTbl.Insert(r.finalLSN, v.NewRow); err != nil {
			return fmt.Errorf("could not insert: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
		r.isEmptyTx = false
	case message.Update:
		tblName, chTbl := r.getTable(v.RelationOID)
		if chTbl == nil || r.skipTableMessage(tblName) {
			break
		}

		if mergeIsNeeded, err := chTbl.Update(r.finalLSN, v.OldRow, v.NewRow); err != nil {
			return fmt.Errorf("could not update: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
		r.isEmptyTx = false
	case message.Delete:
		tblName, chTbl := r.getTable(v.RelationOID)
		if chTbl == nil || r.skipTableMessage(tblName) {
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
			if tblName, chTbl := r.getTable(oid); chTbl == nil || r.skipTableMessage(tblName) {
				continue
			} else {
				if err := chTbl.Truncate(); err != nil {
					return err
				}
			}
		}
		r.isEmptyTx = false
	}

	return nil
}

func (r *Replicator) advanceLSN() {
	r.consumer.AdvanceLSN(r.finalLSN)
}

func (r *Replicator) fetchTableConfig(tx *pgx.Tx, tblName config.PgTableName) (config.Table, error) {
	var err error
	cfg := r.cfg.Tables[tblName]

	cfg.TupleColumns, cfg.PgColumns, err = tableinfo.TablePgColumns(tx, tblName)
	if err != nil {
		return cfg, fmt.Errorf("could not get columns for %s postgres table: %v", tblName.String(), err)
	}

	chColumns, err := tableinfo.TableChColumns(r.chConn, r.cfg.ClickHouse.Database, cfg.ChMainTable)
	if err != nil {
		return cfg, fmt.Errorf("could not get columns for %q clickhouse table: %v", cfg.ChMainTable, err)
	}

	cfg.ColumnMapping = make(map[string]config.ChColumn)
	if len(cfg.Columns) > 0 {
		for pgCol, chCol := range cfg.Columns {
			if chColCfg, ok := chColumns[chCol]; !ok {
				return cfg, fmt.Errorf("could not find %q column in %q clickhouse table", chCol, cfg.ChMainTable)
			} else {
				cfg.ColumnMapping[pgCol] = chColCfg
			}
		}
	} else {
		for _, pgCol := range cfg.TupleColumns {
			if chColCfg, ok := chColumns[pgCol.Name]; !ok {
				return cfg, fmt.Errorf("could not find %q column in %q clickhouse table", pgCol.Name, cfg.ChMainTable)
			} else {
				cfg.ColumnMapping[pgCol.Name] = chColCfg
			}
		}
	}

	return cfg, nil
}
