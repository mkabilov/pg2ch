package decoder

// based on https://github.com/kyleconroy/pgoutput

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

type decoder struct {
	order binary.ByteOrder
	buf   *bytes.Buffer
}

const (
	truncateCascadeBit         = 1
	truncateRestartIdentityBit = 2
)

func (d *decoder) bool() bool { return d.buf.Next(1)[0] != 0 }

func (d *decoder) uint8() uint8   { return d.buf.Next(1)[0] }
func (d *decoder) uint16() uint16 { return d.order.Uint16(d.buf.Next(2)) }
func (d *decoder) uint32() uint32 { return d.order.Uint32(d.buf.Next(4)) }
func (d *decoder) uint64() uint64 { return d.order.Uint64(d.buf.Next(8)) }
func (d *decoder) oid() utils.OID { return utils.OID(d.uint32()) }
func (d *decoder) lsn() utils.LSN { return utils.LSN(d.uint64()) }

func (d *decoder) int8() int8   { return int8(d.uint8()) }
func (d *decoder) int16() int16 { return int16(d.uint16()) }
func (d *decoder) int32() int32 { return int32(d.uint32()) }
func (d *decoder) int64() int64 { return int64(d.uint64()) }

func (d *decoder) string() string {
	s, err := d.buf.ReadBytes(0)
	if err != nil {
		panic(err)
	}

	return string(s[:len(s)-1])
}

func (d *decoder) timestamp() time.Time {
	micro := int(d.uint64())
	ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

	return ts.Add(time.Duration(micro) * time.Microsecond)
}

func (d *decoder) rowInfo(char byte) bool {
	if d.buf.Next(1)[0] == char {
		return true
	}

	_ = d.buf.UnreadByte()
	return false
}

func (d *decoder) tupledata() []message.Tuple {
	size := int(d.uint16())
	data := make([]message.Tuple, size)
	for i := 0; i < size; i++ {
		switch d.buf.Next(1)[0] {
		case 'n':
			data[i] = message.Tuple{Kind: message.TupleNull, Value: []byte{}}
		case 'u':
			data[i] = message.Tuple{Kind: message.TupleUnchanged, Value: []byte{}}
		case 't':
			vsize := int(d.order.Uint32(d.buf.Next(4)))
			data[i] = message.Tuple{Kind: message.TupleText, Value: d.buf.Next(vsize)}
		}
	}

	return data
}

func (d *decoder) columns() []message.Column {
	size := int(d.uint16())
	data := make([]message.Column, size)
	for i := 0; i < size; i++ {
		data[i] = message.Column{}
		data[i].IsKey = d.bool()
		data[i].Name = d.string()
		data[i].TypeOID = d.oid()
		data[i].Mode = d.int32()
	}

	return data
}

// Parse a logical replication message.
// See https://www.postgresql.org/docs/current/static/protocol-logicalrep-message-formats.html
func Parse(src []byte) (message.Message, error) {
	msgType := src[0]
	d := &decoder{order: binary.BigEndian, buf: bytes.NewBuffer(src[1:])}
	switch msgType {
	case 'B':
		m := message.Begin{
			Raw: make([]byte, len(src)),
		}
		copy(m.Raw, src)

		m.FinalLSN = d.lsn()
		m.Timestamp = d.timestamp()
		m.XID = d.int32()

		return m, nil
	case 'C':
		m := message.Commit{
			Raw: make([]byte, len(src)),
		}
		copy(m.Raw, src)

		m.Flags = d.uint8()
		m.LSN = d.lsn()
		m.TransactionLSN = d.lsn()
		m.Timestamp = d.timestamp()

		return m, nil
	case 'O':
		m := message.Origin{
			Raw: make([]byte, len(src)),
		}
		copy(m.Raw, src)

		m.LSN = d.lsn()
		m.Name = d.string()

		return m, nil
	case 'R':
		m := message.Relation{
			Raw: make([]byte, len(src)),
		}
		copy(m.Raw, src)

		m.OID = d.oid()
		m.Namespace = d.string()
		m.Name = d.string()
		m.ReplicaIdentity = message.ReplicaIdentity(d.uint8())
		m.Columns = d.columns()

		return m, nil
	case 'Y':
		m := message.Type{
			Raw: make([]byte, len(src)),
		}
		copy(m.Raw, src)

		m.OID = d.oid()
		m.Namespace = d.string()
		m.Name = d.string()

		return m, nil
	case 'I':
		m := message.Insert{
			Raw: make([]byte, len(src)),
		}
		copy(m.Raw, src)

		m.RelationOID = d.oid()
		m.IsNew = d.uint8() == 'N'
		m.NewRow = d.tupledata()

		return m, nil
	case 'U':
		m := message.Update{
			Raw: make([]byte, len(src)),
		}
		copy(m.Raw, src)

		m.RelationOID = d.oid()
		m.IsKey = d.rowInfo('K')
		m.IsOld = d.rowInfo('O')
		if m.IsKey || m.IsOld {
			m.OldRow = d.tupledata()
		}
		m.IsNew = d.uint8() == 'N'
		m.NewRow = d.tupledata()

		return m, nil
	case 'D':
		m := message.Delete{
			Raw: make([]byte, len(src)),
		}
		copy(m.Raw, src)

		m.RelationOID = d.oid()
		m.IsKey = d.rowInfo('K')
		m.IsOld = d.rowInfo('O')
		m.OldRow = d.tupledata()

		return m, nil
	case 'T':
		m := message.Truncate{
			Raw: make([]byte, len(src)),
		}
		copy(m.Raw, src)

		relationsCnt := int(d.uint32())
		options := d.uint8()
		m.Cascade = options&truncateCascadeBit == 1
		m.RestartIdentity = options&truncateRestartIdentityBit == 1

		m.RelationOIDs = make([]utils.OID, relationsCnt)
		for i := 0; i < relationsCnt; i++ {
			m.RelationOIDs[i] = d.oid()
		}

		return m, nil
	default:
		return nil, fmt.Errorf("unknown message type for %s (%d)", []byte{msgType}, msgType)
	}
}
