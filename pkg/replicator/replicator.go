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

type ClickHouseTable interface {
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

	tables       map[string]ClickHouseTable
	oidName      map[utils.OID]string
	tempSlotName string

	finalLSN utils.LSN
	startLSN utils.LSN // lsn to start consuming changes from

	tablesToMergeMutex *sync.Mutex
	tablesToMerge      map[string]struct{} // tables to be merged
	inTxTables         map[string]struct{} // tables inside running tx
	curTxMergeIsNeeded bool                // if tables in the current transaction are needed to be merged
	stateLSNfp         *os.File
}

type state struct {
	StartLSN utils.LSN `yaml:"start_lsn"`
}

var errNoStateFile = errors.New("no state file")

func New(cfg config.Config) *Replicator {
	r := Replicator{
		cfg:     cfg,
		tables:  make(map[string]ClickHouseTable),
		oidName: make(map[utils.OID]string),
		errCh:   make(chan error),

		tablesToMergeMutex: &sync.Mutex{},
		tablesToMerge:      make(map[string]struct{}),
		inTxTables:         make(map[string]struct{}),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return &r
}

func (r *Replicator) newTable(tableName string) (ClickHouseTable, error) {
	tbl := r.cfg.Tables[tableName]
	switch tbl.Engine {
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
		if tbl.VerColumn == "" {
			return nil, fmt.Errorf("ReplacingMergeTree requires version column to be set")
		}

		return tableengines.NewReplacingMergeTree(r.ctx, r.chConn, tableName, tbl), nil
	case config.MergeTree:
		return tableengines.NewMergeTree(r.ctx, r.chConn, tableName, tbl), nil
	case config.CollapsingMergeTree:
		if tbl.SignColumn == "" {
			return nil, fmt.Errorf("CollapsingMergeTree requires sign column to be set")
		}

		return tableengines.NewCollapsingMergeTree(r.ctx, r.chConn, tableName, tbl), nil
	}

	return nil, fmt.Errorf("%s table engine is not implemented", tbl.Engine)
}

func (r *Replicator) checkPgSlotAndPub(tx *pgx.Tx) error {
	var slotExists, pubExists bool

	err := tx.QueryRow("select "+
		"exists(select 1 from pg_replication_slots where slot_name = $1) as slot_exists, "+
		"exists(select 1 from pg_publication where pubname = $2) as pub_exists",
		r.cfg.Pg.ReplicationSlotName, r.cfg.Pg.PublicationName).Scan(&slotExists, &pubExists)

	if err != nil {
		return fmt.Errorf("could not query: %v", err)
	}

	errMsg := ""

	if !slotExists {
		errMsg += fmt.Sprintf("slot %q does not exist", r.cfg.Pg.ReplicationSlotName)
	}

	if !pubExists {
		if errMsg != "" {
			errMsg += " and "
		}
		errMsg += fmt.Sprintf("publication %q does not exist", r.cfg.Pg.PublicationName)
	}

	if errMsg != "" {
		return fmt.Errorf(errMsg)
	}

	return nil
}

func (r *Replicator) syncTables(tx *pgx.Tx) error {
	lsn, err := r.pgCreateRepSlot(tx)
	if err != nil {
		return fmt.Errorf("could not create replication slot: %v", err)
	}

	for tableName, tbl := range r.tables {
		if !r.cfg.Tables[tableName].SkipInitSync {
			log.Printf("Syncing %q table", tableName)
		}

		if err := tbl.Sync(tx); err != nil {
			return fmt.Errorf("could not sync %q: %v", tableName, err)
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

func (r *Replicator) Run() error {
	if err := r.chConnect(); err != nil {
		return fmt.Errorf("could not connect to clickhouse: %v", err)
	}
	defer r.chDisconnect()

	for tableName := range r.cfg.Tables {
		tbl, err := r.newTable(tableName)
		if err != nil {
			return fmt.Errorf("could not instantiate table: %v", err)
		}

		r.tables[tableName] = tbl
	}

	if err := r.pgConnect(); err != nil {
		return fmt.Errorf("could not connecto postgresql: %v", err)
	}

	tx, err := r.pgBegin()
	if err != nil {
		return err
	}

	if err := r.getCurrentState(); err == errNoStateFile { // sync is needed
		if err := r.syncTables(tx); err != nil {
			return fmt.Errorf("could not sync tables: %v", err)
		}
		if err := r.saveCurrentState(); err != nil {
			return fmt.Errorf("could not init state file: %v", err)
		}
	} else if err != nil {
		return fmt.Errorf("could not get start LSN: %v", err)
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
			fmt.Printf("could not init %s: %v", tblName, err)
		}
	}

	r.consumer = consumer.New(r.ctx, r.errCh, r.cfg.Pg.ConnConfig, r.cfg.Pg.ReplicationSlotName, r.cfg.Pg.PublicationName, r.startLSN)

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
			log.Printf("could not flush %s to main table: %v", tblName, err)
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
	ticker := time.NewTicker(r.cfg.InactivityMergeTimeout)

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
				   c.relname,
				   c.relreplident
			from pg_class c
				   join pg_namespace n on n.oid = c.relnamespace
       			   join pg_publication_tables pub on (c.relname = pub.tablename and n.nspname = pub.schemaname)
			where 
				c.relkind = 'r'
				and pub.pubname = $1`, r.cfg.Pg.PublicationName)

	if err != nil {
		return fmt.Errorf("could not exec: %v", err)
	}

	for rows.Next() {
		var (
			oid             utils.OID
			name            string
			replicaIdentity message.ReplicaIdentity
		)

		if err := rows.Scan(&oid, &name, &replicaIdentity); err != nil {
			return fmt.Errorf("could not scan: %v", err)
		}

		if _, ok := r.cfg.Tables[name]; ok && replicaIdentity != message.ReplicaIdentityFull {
			return fmt.Errorf("table %s must have FULL replica identity(currently it is %q)", name, replicaIdentity)
		}

		r.oidName[oid] = name
	}

	return nil
}

func (r *Replicator) chConnect() error {
	var err error

	r.chConn, err = sql.Open("clickhouse", r.cfg.CHConnectionString)
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

	r.pgConn, err = pgx.Connect(r.cfg.Pg.Merge(pgx.ConnConfig{
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

func (r *Replicator) pgCreateRepSlot(tx *pgx.Tx) (utils.LSN, error) {
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

func (r *Replicator) getTable(oid utils.OID) ClickHouseTable {
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
			return fmt.Errorf("could not commit %q table: %v", tblName, err)
		}

		delete(r.tablesToMerge, tblName)
	}
	r.advanceLSN()

	return nil
}

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
		r.inTxTables = make(map[string]struct{})
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
