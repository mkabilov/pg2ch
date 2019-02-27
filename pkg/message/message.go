package message

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"

	"github.com/mkabilov/pg2ch/pkg/utils"
)

type (
	MType           int
	ReplicaIdentity uint8
	TupleKind       uint8
)

const (
	ReplicaIdentityDefault ReplicaIdentity = 'd'
	ReplicaIdentityNothing                 = 'n'
	ReplicaIdentityIndex                   = 'i'
	ReplicaIdentityFull                    = 'f'

	TupleNull    TupleKind = 'n' // Identifies the data as NULL value.
	TupleToasted           = 'u' // Identifies unchanged TOASTed value (the actual value is not sent).
	TupleText              = 't' // Identifies the data as text formatted value.

	MsgInsert MType = iota
	MsgUpdate
	MsgDelete
	MsgBegin
	MsgCommit
	MsgRelation
	MsgType
	MsgOrigin
	MsgTruncate
)

var (
	replicaIdentities = map[ReplicaIdentity]string{
		ReplicaIdentityDefault: "default",
		ReplicaIdentityIndex:   "index",
		ReplicaIdentityNothing: "nothing",
		ReplicaIdentityFull:    "full",
	}

	typeNames = map[MType]string{
		MsgBegin:    "begin",
		MsgRelation: "relation",
		MsgUpdate:   "update",
		MsgInsert:   "insert",
		MsgDelete:   "delete",
		MsgCommit:   "commit",
		MsgOrigin:   "origin",
		MsgType:     "type",
		MsgTruncate: "truncate",
	}
)

type Row []Tuple

type Message interface {
	fmt.Stringer

	MsgType() MType
	RawData() []byte
}

type NamespacedName struct {
	Namespace string `yaml:"Namespace"`
	Name      string `yaml:"Name"`
}

type Column struct {
	IsKey   bool      `yaml:"IsKey"` // column as part of the key.
	Name    string    `yaml:"Name"`  // Name of the column.
	TypeOID utils.OID `yaml:"OID"`   // OID of the column's data type.
	Mode    int32     `yaml:"Mode"`  // OID modifier of the column (atttypmod).
}

type Tuple struct {
	Kind  TupleKind
	Value []byte
}

type Begin struct {
	Raw       []byte
	FinalLSN  utils.LSN // LSN of the record that lead to this xact to be committed
	Timestamp time.Time // Commit timestamp of the transaction
	XID       int32     // Xid of the transaction.
}

type Commit struct {
	Raw            []byte
	Flags          uint8     // Flags; currently unused (must be 0)
	LSN            utils.LSN // The LastLSN of the commit.
	TransactionLSN utils.LSN // LSN pointing to the end of the commit record + 1
	Timestamp      time.Time // Commit timestamp of the transaction
}

type Origin struct {
	Raw  []byte
	LSN  utils.LSN // The last LSN of the commit on the origin server.
	Name string
}

type Relation struct {
	NamespacedName `yaml:"NamespacedName"`

	Raw             []byte          `yaml:"-"`
	OID             utils.OID       `yaml:"OID"`             // OID of the relation.
	ReplicaIdentity ReplicaIdentity `yaml:"ReplicaIdentity"` // Replica identity
	Columns         []Column        `yaml:"Columns"`         // Columns
}

type Insert struct {
	Raw         []byte
	RelationOID utils.OID // OID of the relation corresponding to the OID in the relation message.
	IsNew       bool      // Identifies tuple as a new tuple.

	NewRow Row
}

type Update struct {
	Raw         []byte
	RelationOID utils.OID // OID of the relation corresponding to the OID in the relation message.
	IsKey       bool      // OldRow contains columns which are part of REPLICA IDENTITY index.
	IsOld       bool      // OldRow contains old tuple in case of REPLICA IDENTITY set to FULL
	IsNew       bool      // Identifies tuple as a new tuple.

	OldRow Row
	NewRow Row
}

type Delete struct {
	Raw         []byte
	RelationOID utils.OID // OID of the relation corresponding to the OID in the relation message.
	IsKey       bool      // OldRow contains columns which are part of REPLICA IDENTITY index.
	IsOld       bool      // OldRow contains old tuple in case of REPLICA IDENTITY set to FULL

	OldRow Row
}

type Truncate struct {
	Raw             []byte
	Cascade         bool
	RestartIdentity bool
	RelationOIDs    []utils.OID
}

type Type struct {
	NamespacedName

	Raw []byte
	OID utils.OID // OID of the data type
}

type queryRunner interface {
	QueryRow(sql string, args ...interface{}) *pgx.Row
	Query(sql string, args ...interface{}) (*pgx.Rows, error)
}

func (t MType) String() string {
	str, ok := typeNames[t]
	if !ok {
		return "unknown"
	}

	return str
}

func (Begin) MsgType() MType    { return MsgBegin }
func (Relation) MsgType() MType { return MsgRelation }
func (Update) MsgType() MType   { return MsgUpdate }
func (Insert) MsgType() MType   { return MsgInsert }
func (Delete) MsgType() MType   { return MsgDelete }
func (Commit) MsgType() MType   { return MsgCommit }
func (Origin) MsgType() MType   { return MsgOrigin }
func (Type) MsgType() MType     { return MsgType }
func (Truncate) MsgType() MType { return MsgTruncate }

func (m Begin) RawData() []byte    { return m.Raw }
func (m Relation) RawData() []byte { return m.Raw }
func (m Update) RawData() []byte   { return m.Raw }
func (m Insert) RawData() []byte   { return m.Raw }
func (m Delete) RawData() []byte   { return m.Raw }
func (m Commit) RawData() []byte   { return m.Raw }
func (m Origin) RawData() []byte   { return m.Raw }
func (m Type) RawData() []byte     { return m.Raw }
func (m Truncate) RawData() []byte { return m.Raw }

func (t Tuple) String() string {
	switch t.Kind {
	case TupleText:
		return utils.QuoteLiteral(string(t.Value))
	case TupleNull:
		return "null"
	case TupleToasted:
		return "[toasted value]"
	}

	return "unknown"
}

func (m Begin) String() string {
	return fmt.Sprintf("FinalLSN:%s Timestamp:%v XID:%d",
		m.FinalLSN.String(), m.Timestamp.Format(time.RFC3339), m.XID)
}

func (m Relation) String() string {
	parts := make([]string, 0)

	parts = append(parts, fmt.Sprintf("OID:%s", m.OID))
	parts = append(parts, fmt.Sprintf("Name:%s", m.NamespacedName))
	parts = append(parts, fmt.Sprintf("RepIdentity:%s", m.ReplicaIdentity))

	columns := make([]string, 0)
	for _, c := range m.Columns {
		var isKey, mode string

		if c.IsKey {
			isKey = " key"
		}

		if c.Mode != -1 {
			mode = fmt.Sprintf("atttypmod:%v", c.Mode)
		}
		colStr := fmt.Sprintf("%q (type:%s)%s%s", c.Name, c.TypeOID, isKey, mode)
		columns = append(columns, colStr)
	}

	parts = append(parts, fmt.Sprintf("Columns:[%s]", strings.Join(columns, ", ")))

	return strings.Join(parts, " ")
}

func (m Update) String() string {
	parts := make([]string, 0)
	newValues := make([]string, 0)
	oldValues := make([]string, 0)

	parts = append(parts, fmt.Sprintf("relOID:%s", m.RelationOID))

	if m.IsKey {
		parts = append(parts, "key")
	}
	if m.IsOld {
		parts = append(parts, "old")
	}
	if m.IsNew {
		parts = append(parts, "new")
	}

	for _, r := range m.NewRow {
		newValues = append(newValues, r.String())
	}

	for _, r := range m.OldRow {
		oldValues = append(oldValues, r.String())
	}

	if len(newValues) > 0 {
		parts = append(parts, fmt.Sprintf("newValues:[%s]", strings.Join(newValues, ", ")))
	}

	if len(oldValues) > 0 {
		parts = append(parts, fmt.Sprintf("oldValues:[%s]", strings.Join(oldValues, ", ")))
	}

	return strings.Join(parts, " ")
}

func (m Insert) String() string {
	parts := make([]string, 0)
	newValues := make([]string, 0)

	parts = append(parts, fmt.Sprintf("relOID:%s", m.RelationOID))

	if m.IsNew {
		parts = append(parts, "new")
	}

	for _, r := range m.NewRow {
		newValues = append(newValues, r.String())
	}

	if len(newValues) > 0 {
		parts = append(parts, fmt.Sprintf("values:[%s]", strings.Join(newValues, ", ")))
	}

	return strings.Join(parts, " ")
}

func (m Delete) String() string {
	parts := make([]string, 0)
	oldValues := make([]string, 0)

	parts = append(parts, fmt.Sprintf("relOID:%s", m.RelationOID))

	if m.IsKey {
		parts = append(parts, "key")
	}
	if m.IsOld {
		parts = append(parts, "old")
	}

	for _, r := range m.OldRow {
		oldValues = append(oldValues, r.String())
	}

	if len(oldValues) > 0 {
		parts = append(parts, fmt.Sprintf("oldValues:[%s]", strings.Join(oldValues, ", ")))
	}

	return strings.Join(parts, " ")
}

func (m Commit) String() string {
	return fmt.Sprintf("LSN:%s Timestamp:%v TxEndLSN:%s",
		m.LSN, m.Timestamp.Format(time.RFC3339), m.TransactionLSN)
}

func (m Origin) String() string {
	return fmt.Sprintf("LSN:%s Name:%s", m.LSN, m.Name)
}

func (m Type) String() string {
	return fmt.Sprintf("OID:%s Name:%s", m.OID, m.NamespacedName)
}

func (m Truncate) String() string {
	parts := make([]string, 0)
	oids := make([]string, 0)

	if m.Cascade {
		parts = append(parts, "cascade")
	}

	if m.RestartIdentity {
		parts = append(parts, "restart_identity")
	}

	for _, oid := range m.RelationOIDs {
		oids = append(oids, oid.String())
	}

	if len(oids) > 0 {
		parts = append(parts, fmt.Sprintf("tableOids:[%s]", strings.Join(oids, ", ")))
	}

	return strings.Join(parts, " ")
}

func (tr Truncate) SQL() string {
	//TODO
	return ""
}

func (ins Insert) SQL(rel Relation) string {
	values := make([]string, 0)
	names := make([]string, 0)
	for i, v := range rel.Columns {
		names = append(names, pgx.Identifier{v.Name}.Sanitize())
		if ins.NewRow[i].Kind == TupleText {
			values = append(values, utils.QuoteLiteral(string(ins.NewRow[i].Value)))
		} else if ins.NewRow[i].Kind == TupleNull {
			values = append(values, "null")
		}
	}

	return fmt.Sprintf("insert into %s (%s) values (%s);",
		rel.Sanitize(),
		strings.Join(names, ", "),
		strings.Join(values, ", "))
}

func (upd Update) SQL(rel Relation) string {
	values := make([]string, 0)
	cond := make([]string, 0)

	for i, v := range rel.Columns {
		if upd.NewRow[i].Kind == TupleNull {
			values = append(values, fmt.Sprintf("%s = null", pgx.Identifier{string(v.Name)}.Sanitize()))
		} else if upd.NewRow[i].Kind == TupleText {
			values = append(values, fmt.Sprintf("%s = %s",
				pgx.Identifier{string(v.Name)}.Sanitize(),
				utils.QuoteLiteral(string(upd.NewRow[i].Value))))
		}

		if upd.IsKey || upd.IsOld {
			if upd.OldRow[i].Kind == TupleText {
				cond = append(cond, fmt.Sprintf("%s = %s",
					pgx.Identifier{string(v.Name)}.Sanitize(),
					utils.QuoteLiteral(string(upd.OldRow[i].Value))))
			} else if upd.OldRow[i].Kind == TupleNull {
				cond = append(cond, fmt.Sprintf("%s is null", pgx.Identifier{string(v.Name)}.Sanitize()))
			}
		} else {
			if upd.NewRow[i].Kind == TupleText && v.IsKey {
				cond = append(cond, fmt.Sprintf("%s = %s",
					pgx.Identifier{string(v.Name)}.Sanitize(),
					utils.QuoteLiteral(string(upd.NewRow[i].Value))))
			} else if upd.NewRow[i].Kind == TupleNull && v.IsKey {
				cond = append(cond, fmt.Sprintf("%s is null", pgx.Identifier{string(v.Name)}.Sanitize()))
			}
		}
	}

	sql := fmt.Sprintf("update %s set %s", rel.Sanitize(), strings.Join(values, ", "))
	if len(cond) > 0 {
		sql += " where " + strings.Join(cond, " and ")
	}
	sql += ";"

	return sql
}

func (del Delete) SQL(rel Relation) string {
	cond := make([]string, 0)
	for i, v := range rel.Columns {
		if del.OldRow[i].Kind == TupleText {
			cond = append(cond, fmt.Sprintf("%s = %s",
				pgx.Identifier{string(v.Name)}.Sanitize(),
				utils.QuoteLiteral(string(del.OldRow[i].Value))))
		}
	}

	return fmt.Sprintf("delete from %s where %s;", rel.Sanitize(), strings.Join(cond, " and "))
}

func (rel Relation) SQL(oldRel Relation) string {
	sqlCommands := make([]string, 0)

	quotedTableName := pgx.Identifier{rel.Namespace, rel.Name}.Sanitize()
	oldTableName := pgx.Identifier{rel.Namespace, rel.Name}.Sanitize()

	if oldTableName != quotedTableName {
		sqlCommands = append(sqlCommands, fmt.Sprintf("alter table %s rename to %s", oldTableName, quotedTableName))
	}

	newColumns := make([]Column, 0)
	deletedColumns := make(map[string]Column)
	alteredColumns := make(map[Column]Column)
	for _, c := range oldRel.Columns {
		deletedColumns[c.Name] = c
	}

	for id, col := range rel.Columns {
		oldCol, ok := deletedColumns[col.Name]
		if !ok {
			newColumns = append(newColumns, col)
			break
		}
		if oldCol.TypeOID != col.TypeOID || oldCol.Mode != col.Mode {
			alteredColumns[oldCol] = rel.Columns[id]
		}

		delete(deletedColumns, col.Name)
	}

	for _, col := range deletedColumns {
		sqlCommands = append(sqlCommands, fmt.Sprintf("alter table %s drop column %s;", quotedTableName, pgx.Identifier{col.Name}.Sanitize()))
	}

	for _, col := range newColumns {
		typMod := "NULL"
		if col.Mode > 0 {
			typMod = fmt.Sprintf("%d", col.Mode)
		}
		sqlCommands = append(sqlCommands,
			fmt.Sprintf(`do $_$begin execute 'alter table %s add column %s ' || format_type(%d, %s); end$_$;`,
				quotedTableName, pgx.Identifier{col.Name}.Sanitize(), col.TypeOID, typMod))
	}

	for oldCol, newCol := range alteredColumns {
		typMod := "NULL"
		if newCol.Mode > 0 {
			typMod = fmt.Sprintf("%d", newCol.Mode)
		}

		sqlCommands = append(sqlCommands,
			fmt.Sprintf(`do $_$begin execute 'alter table %s alter column %s type ' || format_type(%d, %s); end$_$;`,
				quotedTableName, pgx.Identifier{oldCol.Name}.Sanitize(), newCol.TypeOID, typMod))
	}

	if oldRel.ReplicaIdentity != rel.ReplicaIdentity {
		sqlCommands = append(sqlCommands, fmt.Sprintf("alter table %s replica identity %s;", quotedTableName, replicaIdentities[rel.ReplicaIdentity]))
	}

	return strings.Join(sqlCommands, " ")
}

func (r ReplicaIdentity) String() string {
	if name, ok := replicaIdentities[r]; !ok {
		return replicaIdentities[ReplicaIdentityDefault]
	} else {
		return name
	}
}

func (r ReplicaIdentity) MarshalYAML() (interface{}, error) {
	return replicaIdentities[r], nil
}

func (r *ReplicaIdentity) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}

	for k, v := range replicaIdentities {
		if val == v {
			*r = k
			return nil
		}
	}

	return fmt.Errorf("unknown replica identity: %q", val)
}

func (dst *ReplicaIdentity) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		return nil
	}

	*dst = ReplicaIdentity(uint8(src[0]))

	return nil
}

func (t TupleKind) String() string {
	switch t {
	case TupleNull:
		return "null"
	case TupleToasted:
		return "toasted"
	case TupleText:
		return "text"
	}

	return "unknown"
}

func (n NamespacedName) String() string {
	if n.Namespace == "public" {
		return n.Name
	}

	return strings.Join([]string{n.Namespace, n.Name}, ".")
}

func (n NamespacedName) Sanitize() string {
	return pgx.Identifier{n.Namespace, n.Name}.Sanitize()
}

func (rel Relation) Structure() string {
	result := fmt.Sprintf("%s (OID: %v)", rel.NamespacedName, rel.OID)

	cols := make([]string, 0)
	for _, c := range rel.Columns {
		cols = append(cols, fmt.Sprintf("%s(%v)", c.Name, c.TypeOID))
	}

	if len(cols) > 0 {
		result += fmt.Sprintf(" Columns: %s", strings.Join(cols, ", "))
	}

	return result
}

func (rel *Relation) fetchColumns(conn queryRunner) error {
	var columns []Column

	if rel.OID == utils.InvalidOID {
		return fmt.Errorf("table has no oid")
	}

	// query taken from fetch_remote_table_info (src/backend/replication/logical/tablesync.c)
	query := fmt.Sprintf(`
	  SELECT a.attname,
	       a.atttypid,
	       a.atttypmod,
	       coalesce(a.attnum = ANY(i.indkey), false)
	  FROM pg_catalog.pg_attribute a
	  LEFT JOIN pg_catalog.pg_index i
	       ON (i.indexrelid = pg_get_replica_identity_index(%[1]d))
	  WHERE a.attnum > 0::pg_catalog.int2
	  AND NOT a.attisdropped
	  AND a.attrelid = %[1]d
      ORDER BY a.attnum`, rel.OID)

	rows, err := conn.Query(query)
	if err != nil {
		return fmt.Errorf("could not execute query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var column Column

		if err := rows.Scan(&column.Name, &column.TypeOID, &column.Mode, &column.IsKey); err != nil {
			return fmt.Errorf("could not scan: %v", err)
		}

		columns = append(columns, column)
	}

	rel.Columns = columns

	return nil
}

// FetchByOID fetches relation info from the database by oid
func (rel *Relation) FetchByOID(conn queryRunner, oid utils.OID) error {
	var relreplident, tableName, schemaName string

	row := conn.QueryRow(fmt.Sprintf(`select relnamespace::regnamespace::text, relname, relreplident::text from pg_class where oid = %d`, oid))
	if err := row.Scan(&schemaName, &tableName, &relreplident); err != nil {
		return fmt.Errorf("could not fetch replica identity: %v", err)
	}
	rel.Name = tableName
	rel.Namespace = schemaName
	rel.ReplicaIdentity = ReplicaIdentity(relreplident[0])
	rel.OID = oid

	if err := rel.fetchColumns(conn); err != nil {
		return fmt.Errorf("could not fetch columns: %v", err)
	}

	return nil
}

// FetchByName fetches relation info from the database by name
func (rel *Relation) FetchByName(conn queryRunner, tableName NamespacedName) error {
	var (
		relreplident string
		oid          utils.OID
	)

	row := conn.QueryRow(fmt.Sprintf(`select oid, relreplident::text from pg_class where oid = '%s'::regclass::oid`, tableName.Sanitize()))
	if err := row.Scan(&oid, &relreplident); err != nil {
		return fmt.Errorf("could not fetch replica identity: %v", err)
	}
	rel.NamespacedName = tableName
	rel.ReplicaIdentity = ReplicaIdentity(relreplident[0])
	rel.OID = oid

	if err := rel.fetchColumns(conn); err != nil {
		return fmt.Errorf("could not fetch columns: %v", err)
	}

	return nil
}

// Equals checks if rel2 is the same as rel. except for Raw field
func (rel *Relation) Equals(rel2 *Relation) bool {
	if rel.OID != rel2.OID {
		return false
	}

	if rel.NamespacedName != rel2.NamespacedName {
		return false
	}

	if rel.ReplicaIdentity != rel2.ReplicaIdentity {
		return false
	}

	if len(rel.Columns) != len(rel2.Columns) {
		return false
	}

	for i := range rel.Columns {
		if rel.Columns[i] != rel2.Columns[i] {
			return false
		}
	}

	return true
}
