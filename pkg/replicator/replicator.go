package replicator

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
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

	tables       map[config.NamespacedName]clickHouseTable
	oidName      map[utils.OID]config.NamespacedName
	tempSlotName string

	finalLSN utils.LSN
	startLSN utils.LSN // lsn to start consuming changes from

	tablesToMergeMutex *sync.Mutex
	tablesToMerge      map[config.NamespacedName]struct{} // tables to be merged
	inTxTables         map[config.NamespacedName]struct{} // tables inside running tx
	curTxMergeIsNeeded bool                               // if tables in the current transaction are needed to be merged
	stateLSNfp         *os.File
}

type state struct {
	StartLSN utils.LSN `yaml:"start_lsn"`
}

var errNoStateFile = errors.New("no state file")

func New(cfg config.Config) *Replicator {
	r := Replicator{
		cfg:     cfg,
		tables:  make(map[config.NamespacedName]clickHouseTable),
		oidName: make(map[utils.OID]config.NamespacedName),
		errCh:   make(chan error),

		tablesToMergeMutex: &sync.Mutex{},
		tablesToMerge:      make(map[config.NamespacedName]struct{}),
		inTxTables:         make(map[config.NamespacedName]struct{}),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return &r
}

func (r *Replicator) newTable(tblName config.NamespacedName, tblConfig config.Table) (clickHouseTable, error) {
	switch tblConfig.Engine {
	//case config.VersionedCollapsingMergeTree:
	//	if tbl.SignColumn == "" {
	//		return nil, fmt.Errorf("VersionedCollapsingMergeTree requires sign column to be set")
	//	}
	//	if tbl.VerColumn == "" {
	//		return nil, fmt.Errorf("VersionedCollapsingMergeTree requires version column to be set")
	//	}
	//
	//	return tableengines.NewVersionedCollapsingMergeTree(r.ctx, r.chConn, tableName, tbl), nil
	case config.ReplacingMergeTree:
		if tblConfig.VerColumn == "" {
			return nil, fmt.Errorf("ReplacingMergeTree requires version column to be set")
		}

		return tableengines.NewReplacingMergeTree(r.ctx, r.chConn, tblName, tblConfig), nil
	case config.MergeTree:
		return tableengines.NewMergeTree(r.ctx, r.chConn, tblName, tblConfig), nil
	case config.CollapsingMergeTree:
		if tblConfig.SignColumn == "" {
			return nil, fmt.Errorf("CollapsingMergeTree requires sign column to be set")
		}

		return tableengines.NewCollapsingMergeTree(r.ctx, r.chConn, tblName, tblConfig), nil
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
		return fmt.Errorf("could not create replication slot: %v", err)
	}

	if err := r.initTables(tx); err != nil {
		return fmt.Errorf("could not init tables: %v", err)
	}

	for tblName, tbl := range r.tables {
		if err := tbl.Sync(tx); err != nil {
			return fmt.Errorf("could not sync %s: %v", tblName.String(), err)
		}
	}

	if err := r.pgDropRepSlot(tx); err != nil {
		return fmt.Errorf("could not drop replication slot: %v", err)
	}
	r.startLSN, r.finalLSN = lsn, lsn

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

	r.stateLSNfp, err = os.OpenFile(r.cfg.LsnStateFilepath, os.O_RDWR, os.ModePerm)
	if os.IsNotExist(err) {
		r.stateLSNfp, err = os.OpenFile(r.cfg.LsnStateFilepath, os.O_WRONLY|os.O_CREATE, os.ModePerm)
		if err != nil {
			return fmt.Errorf("could not create file: %v", err)
		}

		return errNoStateFile
	}

	if err := yaml.NewDecoder(r.stateLSNfp).Decode(&s); err != nil {
		log.Printf("could not decode: %v", err)
		return errNoStateFile
	}

	if !s.StartLSN.IsValid() {
		return errNoStateFile
	}

	r.startLSN = s.StartLSN

	return nil
}

func (r *Replicator) saveCurrentState() error {
	buf := bytes.NewBuffer(nil)

	if err := yaml.NewEncoder(buf).Encode(state{StartLSN: r.finalLSN}); err != nil {
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

	return nil
}

func (r *Replicator) initTables(tx *pgx.Tx) error {
	for tblName := range r.cfg.Tables {
		tblConfig, err := r.tableConfig(tx, tblName)
		if err != nil {
			return fmt.Errorf("could not get %s table config: %v", tblName.String(), err)
		}

		tbl, err := r.newTable(tblName, tblConfig)
		if err != nil {
			return fmt.Errorf("could not instantiate table: %v", err)
		}

		r.tables[tblName] = tbl
	}

	return nil
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

	if err := r.getCurrentState(); err == errNoStateFile { // sync is needed
		if err := r.createReplSlotAndInitTables(tx); err != nil {
			return fmt.Errorf("could not sync tables: %v", err)
		}
		if err := r.saveCurrentState(); err != nil {
			return fmt.Errorf("could not init state file: %v", err)
		}
	} else if err != nil {
		return fmt.Errorf("could not get start LSN: %v", err)
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

	for tblName, tbl := range r.tables {
		if err := tbl.Init(); err != nil {
			fmt.Printf("could not init %s: %v", tblName.String(), err)
		}
	}

	r.consumer = consumer.New(r.ctx, r.errCh, r.cfg.Postgres.ConnConfig, r.cfg.Postgres.ReplicationSlotName, r.cfg.Postgres.PublicationName, r.startLSN)

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

	r.consumer.AdvanceLSN(r.finalLSN)

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

		fqName := config.NamespacedName{SchemaName: schemaName, TableName: tableName}

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
		fmt.Sprintf("tempslot_%d", r.pgConn.PID()), utils.OutputPlugin))

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

func (r *Replicator) getTable(oid utils.OID) clickHouseTable {
	tblName, ok := r.oidName[oid]
	if !ok {
		return nil
	}

	tbl, ok := r.tables[tblName]
	if !ok {
		return nil
	}

	// TODO: skip adding tables with no buffer table
	if _, ok := r.inTxTables[tblName]; !ok {
		r.inTxTables[tblName] = struct{}{}
	}

	if _, ok := r.tablesToMerge[tblName]; !ok {
		r.tablesToMerge[tblName] = struct{}{}
	}

	return tbl
}

func (r *Replicator) mergeTables() error {
	for tblName := range r.tablesToMerge {
		if _, ok := r.inTxTables[tblName]; ok {
			continue
		}

		if err := r.tables[tblName].FlushToMainTable(); err != nil {
			return fmt.Errorf("could not commit %s table: %v", tblName.String(), err)
		}

		delete(r.tablesToMerge, tblName)
	}
	r.advanceLSN()

	return nil
}

// HandleMessage processes the incoming wal message
func (r *Replicator) HandleMessage(msg message.Message, lsn utils.LSN) error {
	if v, ok := msg.(message.Begin); ok {
		r.finalLSN = v.FinalLSN
	}

	if r.finalLSN <= r.startLSN {
		return nil
	}
	r.tablesToMergeMutex.Lock()
	defer r.tablesToMergeMutex.Unlock()

	switch v := msg.(type) {
	case message.Begin:
		r.curTxMergeIsNeeded = false
	case message.Commit:
		if r.curTxMergeIsNeeded {
			if err := r.mergeTables(); err != nil {
				return fmt.Errorf("could not merge tables: %v", err)
			}
		}
		r.inTxTables = make(map[config.NamespacedName]struct{})
	case message.Insert:
		tbl := r.getTable(v.RelationOID)
		if tbl == nil {
			break
		}

		if mergeIsNeeded, err := tbl.Insert(r.finalLSN, v.NewRow); err != nil {
			return fmt.Errorf("could not insert: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
	case message.Update:
		tbl := r.getTable(v.RelationOID)
		if tbl == nil {
			break
		}

		if mergeIsNeeded, err := tbl.Update(r.finalLSN, v.OldRow, v.NewRow); err != nil {
			return fmt.Errorf("could not update: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
	case message.Delete:
		tbl := r.getTable(v.RelationOID)
		if tbl == nil {
			break
		}

		if mergeIsNeeded, err := tbl.Delete(r.finalLSN, v.OldRow); err != nil {
			return fmt.Errorf("could not delete: %v", err)
		} else {
			r.curTxMergeIsNeeded = r.curTxMergeIsNeeded || mergeIsNeeded
		}
	case message.Truncate:
		for _, oid := range v.RelationOIDs {
			if tbl := r.getTable(oid); tbl == nil {
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

	r.consumer.AdvanceLSN(r.finalLSN)
}

func (r *Replicator) tableConfig(tx *pgx.Tx, tblName config.NamespacedName) (config.Table, error) {
	var err error
	cfg := r.cfg.Tables[tblName]

	cfg.PgColumns, err = r.tablePgColumns(tx, tblName)
	if err != nil {
		return cfg, fmt.Errorf("could not get columns for %s postgres table: %v", tblName.String(), err)
	}

	chColumns, err := r.tableChColumns(cfg.MainTable)
	if err != nil {
		return cfg, fmt.Errorf("could not get columns for %q clickhouse table: %v", cfg.MainTable, err)
	}

	cfg.ColumnMapping = make(map[string]config.ChColumn)
	if len(cfg.Columns) > 0 {
		for pgCol, chCol := range cfg.Columns {
			if chColCfg, ok := chColumns[chCol]; !ok {
				return cfg, fmt.Errorf("could not find %q column in %q clickhouse table", chCol, cfg.MainTable)
			} else {
				cfg.ColumnMapping[pgCol] = chColCfg
			}
		}
	} else {
		for _, pgCol := range cfg.PgColumns {
			if chColCfg, ok := chColumns[pgCol]; !ok {
				return cfg, fmt.Errorf("could not find %q column in %q clickhouse table", pgCol, cfg.MainTable)
			} else {
				cfg.ColumnMapping[pgCol] = chColCfg
			}
		}
	}

	return cfg, nil
}

func (r *Replicator) tableChColumns(chTableName string) (map[string]config.ChColumn, error) {
	result := make(map[string]config.ChColumn)

	rows, err := r.chConn.Query("select name, type from system.columns where database = ? and table = ?",
		r.cfg.ClickHouse.Database, chTableName)

	if err != nil {
		return nil, fmt.Errorf("could not query: %v", err)
	}

	for rows.Next() {
		var colName, colType string

		if err := rows.Scan(&colName, &colType); err != nil {
			return nil, fmt.Errorf("could not scan: %v", err)
		}

		chCol := parseChType(colType)
		chCol.Name = colName
		result[colName] = chCol
	}

	return result, nil
}

func (r *Replicator) tablePgColumns(tx *pgx.Tx, tblName config.NamespacedName) ([]string, error) {
	columns := make([]string, 0)

	rows, err := tx.Query(`select a.attname from pg_class c
inner join pg_namespace n on n.oid = c.relnamespace
inner join pg_attribute a on a.attrelid = c.oid
where n.nspname = $1 and c.relname = $2 
	and a.attnum > 0 and a.attisdropped = false
order by attnum`, tblName.SchemaName, tblName.TableName)
	if err != nil {
		return nil, fmt.Errorf("could not query: %v", err)
	}

	for rows.Next() {
		var colName string

		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("could not scan: %v", err)
		}

		columns = append(columns, colName)
	}

	return columns, nil
}

func parseChType(chType string) (col config.ChColumn) {
	col = config.ChColumn{BaseType: chType, IsArray: false, IsNullable: false}

	if ln := len(chType); ln >= 7 {
		if strings.HasPrefix(chType, "Array(Nullable(") {
			col = config.ChColumn{BaseType: chType[15 : ln-2], IsArray: true, IsNullable: true}
		} else if strings.HasPrefix(chType, "Array(") {
			col = config.ChColumn{BaseType: chType[6 : ln-1], IsArray: true, IsNullable: false}
		} else if strings.HasPrefix(chType, "Nullable(") {
			col = config.ChColumn{BaseType: chType[9 : ln-1], IsArray: false, IsNullable: true}
		}
	}

	if strings.HasPrefix(col.BaseType, "FixedString(") {
		col.BaseType = "FixedString"
	}

	return
}
