package replicator

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"github.com/kshvakov/clickhouse"
	"gopkg.in/yaml.v2"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/consumer"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/tableengines"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

type clickHouseTable interface {
	Insert(lsn utils.LSN, new message.Row) (mergeIsNeeded bool, err error)
	Update(lsn utils.LSN, old message.Row, new message.Row) (mergeIsNeeded bool, err error)
	Delete(lsn utils.LSN, old message.Row) (mergeIsNeeded bool, err error)
	Relation(relation message.Relation)
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

	tables       map[config.PgTableName]clickHouseTable
	oidName      map[utils.OID]config.PgTableName
	tempSlotName string

	finalLSN        utils.LSN
	tableLSN        map[config.PgTableName]utils.LSN
	saveStateNeeded bool

	tablesToMergeMutex *sync.Mutex
	tablesToMerge      map[config.PgTableName]struct{} // tables to be merged
	inTxTables         map[config.PgTableName]struct{} // tables inside running tx
	curTxMergeIsNeeded bool                            // if tables in the current transaction are needed to be merged
	stateLSNfp         *os.File
}

type state struct {
	Tables map[config.PgTableName]utils.LSN `yaml:"tables"`
}

func New(cfg config.Config) *Replicator {
	r := Replicator{
		cfg:     cfg,
		tables:  make(map[config.PgTableName]clickHouseTable),
		oidName: make(map[utils.OID]config.PgTableName),
		errCh:   make(chan error),

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
		if tblConfig.VerColumn == "" {
			return nil, fmt.Errorf("ReplacingMergeTree requires version column to be set")
		}

		return tableengines.NewReplacingMergeTree(r.ctx, r.chConn, tblConfig), nil
	case config.MergeTree:
		return tableengines.NewMergeTree(r.ctx, r.chConn, tblConfig), nil
	case config.CollapsingMergeTree:
		if tblConfig.SignColumn == "" {
			return nil, fmt.Errorf("CollapsingMergeTree requires sign column to be set")
		}

		return tableengines.NewCollapsingMergeTree(r.ctx, r.chConn, tblConfig), nil
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

func (r *Replicator) createReplSlotAndInitTables(tx *pgx.Tx) error {
	lsn, err := r.pgCreateTempRepSlot(tx)
	if err != nil {
		return fmt.Errorf("could not create temporary replication slot: %v", err)
	}

	if err := r.initTables(tx); err != nil {
		return fmt.Errorf("could not init tables: %v", err)
	}

	for tblName, tbl := range r.tables {
		if _, ok := r.tableLSN[tblName]; ok {
			continue
		}

		if err := tbl.Sync(tx); err != nil {
			return fmt.Errorf("could not sync %s: %v", tblName.String(), err)
		}
		r.tableLSN[tblName] = lsn
		r.saveStateNeeded = true
	}

	if err := r.pgDropRepSlot(tx); err != nil {
		return fmt.Errorf("could not drop replication slot: %v", err)
	}
	r.finalLSN = lsn

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

func (r *Replicator) getCurrentState() error {
	var (
		s   state
		err error
	)

	r.stateLSNfp, err = os.OpenFile(r.cfg.LsnStateFilepath, os.O_RDWR, os.FileMode(0666))
	if os.IsNotExist(err) {
		r.stateLSNfp, err = os.OpenFile(r.cfg.LsnStateFilepath, os.O_WRONLY|os.O_CREATE, os.FileMode(0666))
		if err != nil {
			return fmt.Errorf("could not create file: %v", err)
		}

		return nil
	}

	if stat, err := r.stateLSNfp.Stat(); err != nil {
		return fmt.Errorf("could not get file stats: %v", err)
	} else if stat.Size() == 0 {
		return nil
	}

	if err := yaml.NewDecoder(r.stateLSNfp).Decode(&s); err != nil {
		log.Printf("could not decode state yaml: %v", err)
		return nil
	}

	if len(s.Tables) != 0 {
		r.tableLSN = s.Tables
	}

	return nil
}

func (r *Replicator) saveCurrentState() error {
	if !r.saveStateNeeded {
		return nil
	}

	buf := bytes.NewBuffer(nil)

	if err := yaml.NewEncoder(buf).Encode(state{Tables: r.tableLSN}); err != nil {
		return fmt.Errorf("could not encode: %v", err)
	}
	if _, err := r.stateLSNfp.Seek(0, 0); err != nil {
		return fmt.Errorf("could not seek: %v", err)
	}

	n, err := buf.WriteTo(r.stateLSNfp)
	if err != nil {
		return fmt.Errorf("could not write to file: %v", err)
	}

	if err := r.stateLSNfp.Truncate(n); err != nil {
		return fmt.Errorf("could not truncate: %v", err)
	}

	if err := utils.SyncFileAndDirectory(r.stateLSNfp); err != nil {
		return fmt.Errorf("could not sync state file %q: %v", r.cfg.LsnStateFilepath, err)
	}
	r.saveStateNeeded = false

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
			fmt.Printf("could not init %s: %v", tblName.String(), err)
		}

		r.tables[tblName] = tbl
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

func (r *Replicator) Run() error {
	if err := r.chConnect(); err != nil {
		return fmt.Errorf("could not connect to clickhouse: %v", err)
	}
	defer r.chDisconnect()

	if err := r.pgConnect(); err != nil {
		return fmt.Errorf("could not connect to postgresql: %v", err)
	}

	tx, err := r.pgBegin()
	if err != nil {
		return err
	}

	if err := r.getCurrentState(); err != nil {
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
		if err := r.createReplSlotAndInitTables(tx); err != nil {
			return fmt.Errorf("could not sync tables: %v", err)
		}
		if err := r.saveCurrentState(); err != nil {
			return fmt.Errorf("could not init state file: %v", err)
		}
	} else if err := r.initTables(tx); err != nil {
		return fmt.Errorf("could not init tables: %v", err)
	}

	// in case of init sync, the replication slot must be created, which must be called before any query
	if err := r.checkPgSlotAndPub(tx); err != nil {
		return err
	}

	if err := r.fetchPgTablesInfo(tx); err != nil {
		return fmt.Errorf("table check failed: %v", err)
	}

	if err := r.pgCommit(tx); err != nil {
		return err
	}

	if err := r.pgConn.Close(); err != nil { // logical replication consumer uses it's own connection
		return fmt.Errorf("could not close pg connection: %v", err)
	}

	startLSN := r.minLSN()

	r.consumer = consumer.New(r.ctx, r.errCh, r.cfg.Postgres.ConnConfig, r.cfg.Postgres.ReplicationSlotName, r.cfg.Postgres.PublicationName, startLSN)

	if err := r.consumer.Run(r); err != nil {
		return err
	}

	go r.logErrCh()
	go r.inactivityMerge()

	r.waitForShutdown()
	r.cancel()
	r.consumer.Wait()

	for tblName, tbl := range r.tables {
		if err := tbl.FlushToMainTable(); err != nil {
			log.Printf("could not flush %s to main table: %v", tblName.String(), err)
		}
	}

	if err := r.saveCurrentState(); err != nil {
		log.Printf("could not save current state: %v", err)
	}

	r.consumer.AdvanceLSN(startLSN)

	if err := r.stateLSNfp.Close(); err != nil {
		log.Printf("could not close state file: %v", err)
	}

	return nil
}

func (r *Replicator) inactivityMerge() {
	ticker := time.NewTicker(r.cfg.InactivityFlushTimeout)

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.tablesToMergeMutex.Lock()
			if err := r.mergeTables(); err != nil {
				select {
				case r.errCh <- fmt.Errorf("could not backgound merge tables: %v", err):
				default:
				}
			}
			r.tablesToMergeMutex.Unlock()
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
		RuntimeParams:        map[string]string{"replication": "database"},
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

func (r *Replicator) pgDropRepSlot(tx *pgx.Tx) error {
	_, err := tx.Exec(fmt.Sprintf("DROP_REPLICATION_SLOT %s", r.tempSlotName))

	return err
}

func (r *Replicator) pgCreateTempRepSlot(tx *pgx.Tx) (utils.LSN, error) {
	var (
		basebackupLSN, snapshotName, plugin sql.NullString
		lsn                                 utils.LSN
	)

	row := tx.QueryRow(fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s USE_SNAPSHOT",
		fmt.Sprintf("clickhouse_tempslot_%d", r.pgConn.PID()), utils.OutputPlugin))

	if err := row.Scan(&r.tempSlotName, &basebackupLSN, &snapshotName, &plugin); err != nil {
		return utils.InvalidLSN, fmt.Errorf("could not scan: %v", err)
	}

	if err := lsn.Parse(basebackupLSN.String); err != nil {
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

	tbl, ok := r.tables[tblName]
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

	return tblName, tbl
}

func (r *Replicator) mergeTables() error {
	r.saveStateNeeded = r.saveStateNeeded || len(r.tablesToMerge) > 0
	for tblName := range r.tablesToMerge {
		if _, ok := r.inTxTables[tblName]; ok {
			continue
		}

		if err := r.tables[tblName].FlushToMainTable(); err != nil {
			return fmt.Errorf("could not commit %s table: %v", tblName.String(), err)
		}

		delete(r.tablesToMerge, tblName)
		r.tableLSN[tblName] = r.finalLSN
	}
	r.advanceLSN()

	return nil
}

// HandleMessage processes the incoming wal message
func (r *Replicator) HandleMessage(msg message.Message, lsn utils.LSN) error {
	r.tablesToMergeMutex.Lock()
	defer r.tablesToMergeMutex.Unlock()

	switch v := msg.(type) {
	case message.Begin:
		r.finalLSN = v.FinalLSN
		r.curTxMergeIsNeeded = false
	case message.Commit:
		if r.curTxMergeIsNeeded {
			if err := r.mergeTables(); err != nil {
				return fmt.Errorf("could not merge tables: %v", err)
			}
		}
		r.inTxTables = make(map[config.PgTableName]struct{})
	case message.Relation:
		tblName, tbl := r.getTable(v.OID)
		if tbl == nil || r.skipTableMessage(tblName) {
			break
		}

		tbl.Relation(v)
	case message.Insert:
		tblName, tbl := r.getTable(v.RelationOID)
		if tbl == nil || r.skipTableMessage(tblName) {
			break
		}

		if mergeIsNeeded, err := tbl.Insert(r.finalLSN, v.NewRow); err != nil {
			return fmt.Errorf("could not insert: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
	case message.Update:
		tblName, tbl := r.getTable(v.RelationOID)
		if tbl == nil || r.skipTableMessage(tblName) {
			break
		}

		if mergeIsNeeded, err := tbl.Update(r.finalLSN, v.OldRow, v.NewRow); err != nil {
			return fmt.Errorf("could not update: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
	case message.Delete:
		tblName, tbl := r.getTable(v.RelationOID)
		if tbl == nil || r.skipTableMessage(tblName) {
			break
		}

		if mergeIsNeeded, err := tbl.Delete(r.finalLSN, v.OldRow); err != nil {
			return fmt.Errorf("could not delete: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
	case message.Truncate:
		for _, oid := range v.RelationOIDs {
			if tblName, tbl := r.getTable(oid); tbl == nil || r.skipTableMessage(tblName) {
				continue
			} else {
				if err := tbl.Truncate(); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (r *Replicator) advanceLSN() {
	if err := r.saveCurrentState(); err != nil {
		select {
		case r.errCh <- err:
		default:
		}

		return
	}

	r.consumer.AdvanceLSN(r.minLSN())
}

func (r *Replicator) fetchTableConfig(tx *pgx.Tx, tblName config.PgTableName) (config.Table, error) {
	var err error
	cfg := r.cfg.Tables[tblName]

	cfg.TupleColumns, cfg.PgColumns, err = utils.TablePgColumns(tx, tblName)
	if err != nil {
		return cfg, fmt.Errorf("could not get columns for %s postgres table: %v", tblName.String(), err)
	}

	chColumns, err := utils.TableChColumns(r.chConn, r.cfg.ClickHouse.Database, cfg.ChMainTable)
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
			if chColCfg, ok := chColumns[pgCol]; !ok {
				return cfg, fmt.Errorf("could not find %q column in %q clickhouse table", pgCol, cfg.ChMainTable)
			} else {
				cfg.ColumnMapping[pgCol] = chColCfg
			}
		}
	}

	return cfg, nil
}
