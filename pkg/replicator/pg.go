package replicator

import (
	"database/sql"
	"fmt"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/pgutils"
	"github.com/mkabilov/pg2ch/pkg/utils/tableinfo"
)

func (r *Replicator) fetchTableOID(tblName config.PgTableName, tx *pgx.Tx) (dbtypes.OID, error) {
	var oid dbtypes.OID
	if err := tx.QueryRow("select $1::regclass::oid", tblName.String()).Scan(&oid); err != nil {
		return 0, err
	}

	return oid, nil
}

func (r *Replicator) pgDeltaConnect() error {
	var err error

	r.pgDeltaConn, err = pgx.Connect(r.pgxConnConfig)
	if err != nil {
		return fmt.Errorf("could not rep connect to pg: %v", err)
	}

	connInfo, err := initPostgresql(r.pgDeltaConn)
	if err != nil {
		return fmt.Errorf("could not fetch conn info: %v", err)
	}
	r.pgDeltaConn.ConnInfo = connInfo

	return nil
}

func (r *Replicator) pgDeltaDisconnect() {
	if err := r.pgDeltaConn.Close(); err != nil {
		r.logger.Warnf("could not close connection to postgresql: %v", err)
	}
}

func tempSlotName(tblName config.PgTableName) string {
	return fmt.Sprintf("%s_%s_%s", config.ApplicationName, tblName.SchemaName, tblName.TableName)
}

func (r *Replicator) pgCreateTempRepSlot(tx *pgx.Tx, slotName string) (dbtypes.LSN, error) {
	var (
		snapshotLSN, snapshotName, plugin sql.NullString
		lsn                               dbtypes.LSN
		tmpSlotName                       sql.NullString
	)

	row := tx.QueryRow(fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s USE_SNAPSHOT",
		slotName, pgutils.OutputPlugin))

	if err := row.Scan(&tmpSlotName, &snapshotLSN, &snapshotName, &plugin); err != nil {
		return dbtypes.InvalidLSN, fmt.Errorf("could not scan: %v", err)
	}

	if err := lsn.Parse(snapshotLSN.String); err != nil {
		return dbtypes.InvalidLSN, fmt.Errorf("could not parse LSN: %v", err)
	}

	if _, err := tx.Exec(fmt.Sprintf("DROP_REPLICATION_SLOT %s", slotName)); err != nil {
		return dbtypes.InvalidLSN, fmt.Errorf("could not drop replication slot: %v", err)
	}

	return lsn, nil
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

func (r *Replicator) pgCommit(tx *pgx.Tx) {
	if err := tx.Commit(); err != nil {
		r.logger.Warnf("could not commit: %v", err)
	}
}

func (r *Replicator) pgRollback(tx *pgx.Tx) {
	if err := tx.Rollback(); err != nil {
		r.logger.Warnf("could not rollback: %v", err)
	}
}

func (r *Replicator) fetchTableConfig(tx *pgx.Tx, tblName config.PgTableName) error {
	var err error
	cfg := r.cfg.Tables[tblName]

	cfg.TupleColumns, cfg.PgColumns, err = tableinfo.TablePgColumns(tx, tblName)
	if err != nil {
		return fmt.Errorf("could not get columns for %s postgres table: %v", tblName.String(), err)
	}

	chColumns, err := tableinfo.TableChColumns(r.chConn, cfg.ChMainTable)
	if err != nil {
		return fmt.Errorf("could not get columns for %q clickhouse table: %v", cfg.ChMainTable, err)
	}

	cfg.ColumnMapping = make(map[string]config.ChColumn)
	if len(cfg.Columns) > 0 {
		for pgCol, chCol := range cfg.Columns {
			if chColCfg, ok := chColumns[chCol]; !ok {
				if cfg.PgColumns[pgCol].IsIstore() {
					cfg.ColumnMapping[pgCol] = config.ChColumn{Name: chCol}
				} else {
					return fmt.Errorf("could not find %q column in %q clickhouse table", chCol, cfg.ChMainTable)
				}
			} else {
				cfg.ColumnMapping[pgCol] = chColCfg
			}
		}
	} else {
		for _, pgCol := range cfg.TupleColumns {
			if chColCfg, ok := chColumns[pgCol.Name]; !ok {
				if cfg.PgColumns[pgCol.Name].IsIstore() {
					cfg.ColumnMapping[pgCol.Name] = config.ChColumn{Name: pgCol.Name}
				} else {
					return fmt.Errorf("could not find %q column in %q clickhouse table", pgCol.Name, cfg.ChMainTable)
				}
			} else {
				cfg.ColumnMapping[pgCol.Name] = chColCfg
			}
		}
	}

	if cfg.ColumnProperties == nil {
		cfg.ColumnProperties = make(map[string]config.ColumnProperty)
	}

	for pgCol := range cfg.ColumnMapping {
		// we can't move that to the config struct because we need actual type of the column
		if _, ok := cfg.ColumnProperties[pgCol]; !ok && cfg.PgColumns[pgCol].IsIstore() {
			cfg.ColumnProperties[pgCol] = config.ColumnProperty{
				IstoreKeysSuffix:   "keys",
				IstoreValuesSuffix: "values",
			}
		}
	}
	cfg.PgTableName = tblName
	return nil
}

func (r *Replicator) pgBegin(pgxConn *pgx.Conn) (*pgx.Tx, error) {
	tx, err := pgxConn.BeginEx(r.ctx, &pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("could not start pg transaction: %v", err)
	}

	return tx, nil
}

func (r *Replicator) minLSN() dbtypes.LSN {
	result := dbtypes.InvalidLSN

	for key := range r.persStorage.Keys(nil) {
		var (
			tblName config.PgTableName
			lsn     dbtypes.LSN
		)

		if err := tblName.ParseKey(key); err != nil {
			continue
		}

		lsn, err := r.persStorage.ReadLSN(key)
		if err != nil {
			r.logger.Warnf("could not read lsn for %q key: %v", key, err)
			continue
		}

		r.logger.Debugf("consuming changes for table %q starting from %s", tblName, lsn)

		if !result.IsValid() || lsn < result {
			result = lsn
		}
	}

	return result
}

func (r *Replicator) pgCheck() error {
	tx, err := r.pgBegin(r.pgDeltaConn)
	if err != nil {
		return fmt.Errorf("could not begin: %v", err)
	}

	if err := r.checkPgSlotAndPub(tx); err != nil {
		return err
	}

	for pgTbl := range r.cfg.Tables {
		var repIdentity message.ReplicaIdentity
		row := tx.QueryRow("select relreplident from pg_class where oid = $1::regclass;", pgTbl.NamespacedName())
		if err := row.Scan(&repIdentity); err != nil {
			return fmt.Errorf("could not fetch replica identity for table %q: %v", pgTbl, err)
		}

		if repIdentity != message.ReplicaIdentityFull {
			return fmt.Errorf("table %q must have 'full' replica identity (currently is %s)", pgTbl.String(), repIdentity)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit: %v", err)
	}

	return nil
}
