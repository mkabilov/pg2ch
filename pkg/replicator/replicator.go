package replicator

import (
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

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/consumer"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/tableengines"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

type ClickHouseTable interface {
	Insert(lsn utils.LSN, new message.Row) (bool, error)
	Update(lsn utils.LSN, old message.Row, new message.Row) (bool, error)
	Delete(lsn utils.LSN, old message.Row) (bool, error)
	Truncate() error
	Sync(*pgx.Tx) error
	Close() error
	FlushToMainTable() error
}

type Replicator struct {
	ctx      context.Context
	cancel   context.CancelFunc
	consumer consumer.Interface
	cfg      config.Config
	errCh    chan error

	pgConn *pgx.Conn
	pgTx   *pgx.Tx
	chConn *sql.DB

	tables       map[string]ClickHouseTable
	oidName      map[utils.OID]string
	tempSlotName string

	finalLSN utils.LSN
	startLSN utils.LSN

	inTxMutex     *sync.Mutex
	inTx          bool
	mergeIsNeeded bool
	txTables      map[string]struct{} // touched in the tx tables
}

func New(cfg config.Config) *Replicator {
	r := Replicator{
		cfg:       cfg,
		tables:    make(map[string]ClickHouseTable),
		oidName:   make(map[utils.OID]string),
		errCh:     make(chan error),
		txTables:  make(map[string]struct{}),
		inTxMutex: &sync.Mutex{},
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
	//	return tableengines.NewVersionedCollapsingMergeTree(r.chConn, tableName, tbl), nil
	case config.ReplacingMergeTree:
		if tbl.VerColumn == "" {
			return nil, fmt.Errorf("ReplacingMergeTree requires version column to be set")
		}

		return tableengines.NewReplacingMergeTree(r.chConn, tableName, tbl), nil
	case config.MergeTree:
		return tableengines.NewMergeTree(r.chConn, tableName, tbl), nil
	case config.CollapsingMergeTree:
		if tbl.SignColumn == "" {
			return nil, fmt.Errorf("CollapsingMergeTree requires sign column to be set")
		}

		return tableengines.NewCollapsingMergeTree(r.chConn, tableName, tbl), nil
	}

	return nil, fmt.Errorf("%s table engine is not implemented", tbl.Engine)
}

func (r *Replicator) checkPg() error {
	var slotExists, pubExists bool

	err := r.pgConn.QueryRow("select "+
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

func (r *Replicator) Run() error {
	if err := r.chConnect(); err != nil {
		return fmt.Errorf("could not connect to clickhouse: %v", err)
	}
	defer r.chDisconnect()

	if err := r.pgConnect(); err != nil {
		return fmt.Errorf("could not connecto postgresql: %v", err)
	}

	if err := r.checkPg(); err != nil {
		return err
	}

	if err := r.pgCreateRepSlot(); err != nil {
		return fmt.Errorf("could not create replication slot: %v", err)
	}
	r.consumer = consumer.New(r.ctx, r.errCh, r.cfg.Pg.ConnConfig, r.cfg.Pg.ReplicationSlotName, r.cfg.Pg.PublicationName, r.startLSN)

	if err := r.fetchTableOIDs(); err != nil {
		return fmt.Errorf("table check failed: %v", err)
	}

	for tableName, tableCfg := range r.cfg.Tables {
		tbl, err := r.newTable(tableName)
		if err != nil {
			return fmt.Errorf("could not instantiate table: %v", err)
		}

		if !tableCfg.SkipInitSync {
			log.Printf("Syncing %q table", tableName)
		}
		r.tables[tableName] = tbl
		if err := tbl.Sync(r.pgTx); err != nil {
			return fmt.Errorf("could not sync %q: %v", tableName, err)
		}
	}

	if err := r.pgDropRepSlot(); err != nil {
		return fmt.Errorf("could not drop replication slot: %v", err)
	}

	if err := r.pgConn.Close(); err != nil { // logical replication consumer uses it's own connection
		return fmt.Errorf("could not close pg connection: %v", err)
	}

	if err := r.consumer.Run(r); err != nil {
		return err
	}

	go r.logErrCh()
	go r.inactivityMerge()

	r.waitForShutdown()
	r.cancel()
	r.consumer.Wait()

	for tblName, tbl := range r.tables {
		if err := tbl.Close(); err != nil {
			log.Printf("could not close %s: %v", tblName, err)
		}
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
			r.inTxMutex.Lock()
			if r.inTx {
				log.Printf("skipping due to inside tx")
				r.inTxMutex.Unlock()
				break
			}
			r.mergeIsNeeded = true

			if err := r.maybeMergeTables(); err != nil {
				r.errCh <- fmt.Errorf("could not backgound merge tables: %v", err)
			}
			r.inTxMutex.Unlock()
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

func (r *Replicator) fetchTableOIDs() error {
	rows, err := r.pgTx.Query(`
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

		err = rows.Scan(&oid, &name, &replicaIdentity)
		if err != nil {
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

func (r *Replicator) pgDropRepSlot() error {
	_, err := r.pgTx.Exec(fmt.Sprintf("DROP_REPLICATION_SLOT %s", r.tempSlotName))

	return err
}

func (r *Replicator) pgCreateRepSlot() error {
	var basebackupLSN, snapshotName, plugin sql.NullString

	tx, err := r.pgConn.BeginEx(r.ctx, &pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("could not start pg transaction: %v", err)
	}

	row := tx.QueryRow(fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s USE_SNAPSHOT",
		fmt.Sprintf("tempslot_%d", r.pgConn.PID()), utils.OutputPlugin))

	if err := row.Scan(&r.tempSlotName, &basebackupLSN, &snapshotName, &plugin); err != nil {
		return fmt.Errorf("could not scan: %v", err)
	}

	if err := r.startLSN.Parse(basebackupLSN.String); err != nil {
		return fmt.Errorf("could not parse LSN: %v", err)
	}

	r.pgTx = tx

	return nil
}

func (r *Replicator) waitForShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)

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

	if _, ok := r.txTables[tblName]; !ok {
		r.txTables[tblName] = struct{}{}
	}

	return tbl
}

func (r *Replicator) maybeMergeTables() error {
	if !r.mergeIsNeeded {
		return nil
	}

	for tblName := range r.txTables {
		if err := r.tables[tblName].FlushToMainTable(); err != nil {
			return fmt.Errorf("could not commit %q table: %v", tblName, err)
		}
	}

	r.mergeIsNeeded = false

	return nil
}

func (r *Replicator) HandleMessage(msg message.Message, lsn utils.LSN) error {
	if lsn < r.startLSN {
		return nil
	}

	switch v := msg.(type) {
	case message.Begin:
		r.inTxMutex.Lock()
		defer r.inTxMutex.Unlock()
		r.inTx = true

		r.finalLSN = v.FinalLSN
		r.txTables = make(map[string]struct{})
	case message.Commit:
		r.consumer.AdvanceLSN(lsn)
		r.inTxMutex.Lock()
		defer func() {
			r.inTx = false
			r.inTxMutex.Unlock()
		}()

		if err := r.maybeMergeTables(); err != nil {
			return fmt.Errorf("could not merge tables: %v", err)
		}
	case message.Insert:
		tbl := r.getTable(v.RelationOID)
		if tbl == nil {
			break
		}

		if mergeIsNeeded, err := tbl.Insert(r.finalLSN, v.NewRow); err != nil {
			return fmt.Errorf("could not insert: %v", err)
		} else {
			r.mergeIsNeeded = r.mergeIsNeeded || mergeIsNeeded
		}
	case message.Update:
		tbl := r.getTable(v.RelationOID)
		if tbl == nil {
			break
		}

		if mergeIsNeeded, err := tbl.Update(r.finalLSN, v.OldRow, v.NewRow); err != nil {
			return fmt.Errorf("could not update: %v", err)
		} else {
			r.mergeIsNeeded = r.mergeIsNeeded || mergeIsNeeded
		}
	case message.Delete:
		tbl := r.getTable(v.RelationOID)
		if tbl == nil {
			break
		}

		if mergeIsNeeded, err := tbl.Delete(r.finalLSN, v.OldRow); err != nil {
			return fmt.Errorf("could not delete: %v", err)
		} else {
			r.mergeIsNeeded = r.mergeIsNeeded || mergeIsNeeded
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
