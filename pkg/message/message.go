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

	TupleNull      TupleKind = 'n' // Identifies the data as NULL value.
	TupleUnchanged           = 'u' // Identifies unchanged TOASTed value (the actual value is not sent).
	TupleText                = 't' // Identifies the data as text formatted value.

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

func (t MType) String() string {
	str, ok := typeNames[t]
	if !ok {
		return "unknown"
	}

	return str
}

func (t Tuple) String() string {
	switch t.Kind {
	case TupleText:
		return utils.QuoteLiteral(string(t.Value))
	case TupleNull:
		return "null"
	case TupleUnchanged:
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

func (r *ReplicaIdentity) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		return nil
	}

	*r = ReplicaIdentity(uint8(src[0]))

	return nil
}

func (t TupleKind) String() string {
	switch t {
	case TupleNull:
		return "null"
	case TupleUnchanged:
		return "[unchanged value]"
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
