package chutils

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/pgtype"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/pgutils"
)

const (
	pgTrue  = 't'
	pgFalse = 'f'

	//ajBool specific value
	ajBoolUnknown = 'u'
	null          = "null"

	timestampLength = 19 // ->2019-06-08 15:50:01<- clickhouse does not support milliseconds
)

var (
	escaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

	pgToChMap = map[string]string{
		dbtypes.PgSmallint:                 dbtypes.ChInt16,
		dbtypes.PgInteger:                  dbtypes.ChInt32,
		dbtypes.PgBigint:                   dbtypes.ChInt64,
		dbtypes.PgCharacterVarying:         dbtypes.ChString,
		dbtypes.PgVarchar:                  dbtypes.ChString,
		dbtypes.PgText:                     dbtypes.ChString,
		dbtypes.PgReal:                     dbtypes.ChFloat32,
		dbtypes.PgDoublePrecision:          dbtypes.ChFloat64,
		dbtypes.PgInterval:                 dbtypes.ChInt32,
		dbtypes.PgBoolean:                  dbtypes.ChUInt8,
		dbtypes.PgDecimal:                  dbtypes.ChDecimal,
		dbtypes.PgNumeric:                  dbtypes.ChDecimal,
		dbtypes.PgCharacter:                dbtypes.ChFixedString,
		dbtypes.PgChar:                     dbtypes.ChFixedString,
		dbtypes.PgJsonb:                    dbtypes.ChString,
		dbtypes.PgJson:                     dbtypes.ChString,
		dbtypes.PgUuid:                     dbtypes.ChUUID,
		dbtypes.PgBytea:                    dbtypes.ChUInt8Array,
		dbtypes.PgInet:                     dbtypes.ChInt64,
		dbtypes.PgTimestamp:                dbtypes.ChDateTime,
		dbtypes.PgTimestampWithTimeZone:    dbtypes.ChDateTime,
		dbtypes.PgTimestampWithoutTimeZone: dbtypes.ChDateTime,
		dbtypes.PgDate:                     dbtypes.ChDate,
		dbtypes.PgTime:                     dbtypes.ChUint32,
		dbtypes.PgTimeWithoutTimeZone:      dbtypes.ChUint32,
		dbtypes.PgTimeWithTimeZone:         dbtypes.ChUint32,

		// adjust gmbh specific types
		dbtypes.PgAdjustIstore:    dbtypes.ChInt32,    // type for istore values
		dbtypes.PgAdjustBigIstore: dbtypes.ChInt64,    // type for bigistore values
		dbtypes.PgAdjustAjTime:    dbtypes.ChDateTime, // adjust time
		dbtypes.PgAdjustAjDate:    dbtypes.ChDate,     // adjust date
		dbtypes.PgAdjustAjBool:    dbtypes.ChUInt8,    // adjust boolean: true, false, unknown
	}

	ajBoolUnkownValue = []byte("2")
	nullStr           = []byte(`\N`)
	istoreNull        = []byte("[]\t[]")
)

// ToClickHouseType converts pg type into clickhouse type
func ToClickHouseType(pgColumn config.PgColumn) (string, error) {
	chType, ok := pgToChMap[pgColumn.BaseType]
	if !ok {
		chType = dbtypes.ChString
	}

	switch pgColumn.BaseType {
	case dbtypes.PgDecimal:
		fallthrough
	case dbtypes.PgNumeric:
		if pgColumn.Ext == nil {
			return "", fmt.Errorf("precision must be specified for the numeric type")
		}
		chType = fmt.Sprintf("%s(%d, %d)", chType, pgColumn.Ext[0], pgColumn.Ext[1])
	case dbtypes.PgCharacter:
		fallthrough
	case dbtypes.PgChar:
		if pgColumn.Ext == nil {
			return "", fmt.Errorf("length must be specified for character type")
		}
		chType = fmt.Sprintf("%s(%d)", chType, pgColumn.Ext[0])
	case dbtypes.PgAdjustCountry:
		fallthrough
	case dbtypes.PgAdjustOsName:
		fallthrough
	case dbtypes.PgAdjustDeviceType:
		chType = fmt.Sprintf("LowCardinality(%s)", chType)
	}

	if pgColumn.IsArray {
		chType = fmt.Sprintf("Array(%s)", chType)
	}

	if pgColumn.IsNullable && !pgColumn.IsArray {
		chType = fmt.Sprintf("Nullable(%s)", chType)
	}

	return chType, nil
}

func GenInsertQuery(tableName config.ChTableName, columns []string) string {
	columnsStr := ""
	queryFormat := "INSERT INTO %s%s FORMAT TabSeparated"
	if columns != nil && len(columns) > 0 {
		columnsStr = "(" + strings.Join(columns, ", ") + ")"
	}

	return fmt.Sprintf(queryFormat, tableName, columnsStr)
}

func convertBaseType(baseType string, tupleData message.Tuple, colProp config.ColumnProperty) []byte {
	switch baseType {
	case dbtypes.PgAdjustIstore:
		fallthrough
	case dbtypes.PgAdjustBigIstore:
		if colProp.FlattenIstore {
			if tupleData.Kind == message.TupleNull {
				return []byte(strings.Repeat("\t\\N", colProp.FlattenIstoreMax-colProp.FlattenIstoreMin+1))[1:]
			}

			return pgutils.IstoreValues(tupleData.Value, colProp.FlattenIstoreMin, colProp.FlattenIstoreMax)
		} else {
			if tupleData.Kind == message.TupleNull {
				return istoreNull
			}

			return pgutils.IstoreToArrays(tupleData.Value)
		}
	case dbtypes.PgAdjustAjBool:
		fallthrough
	case dbtypes.PgBoolean:
		if tupleData.Kind == message.TupleNull {
			if len(colProp.Coalesce) > 0 {
				return colProp.Coalesce
			}
			return nullStr
		}

		switch tupleData.Value[0] {
		case pgTrue:
			return []byte("1")
		case pgFalse:
			return []byte("0")
		case ajBoolUnknown:
			return ajBoolUnkownValue
		}
	case dbtypes.PgTimestampWithTimeZone:
		fallthrough
	case dbtypes.PgTimestampWithoutTimeZone:
		fallthrough
	case dbtypes.PgTimestamp:
		if tupleData.Kind == message.TupleNull {
			if len(colProp.Coalesce) > 0 {
				return colProp.Coalesce
			}
			return nullStr
		}

		return tupleData.Value[:timestampLength]
	case dbtypes.PgTime:
		fallthrough
	case dbtypes.PgTimeWithoutTimeZone:
		//TODO
	case dbtypes.PgTimeWithTimeZone:
		//TODO
	}
	if tupleData.Kind == message.TupleNull {
		if len(colProp.Coalesce) > 0 {
			return colProp.Coalesce
		}

		return nullStr
	}

	return tupleData.Value
}

func ConvertColumn(column config.PgColumn, tupleData message.Tuple, colProp config.ColumnProperty) []byte {
	if !column.IsArray {
		return convertBaseType(column.BaseType, tupleData, colProp)
	}

	switch column.BaseType {
	case dbtypes.PgBigint:
		fallthrough
	case dbtypes.PgInteger:
		return append(append([]byte("["), tupleData.Value[1:len(tupleData.Value)-1]...), ']')
	default:
		val := pgtype.TextArray{}
		if err := val.DecodeText(nil, tupleData.Value); err != nil {
			panic(err)
		}
		res := make([]string, len(val.Elements))
		for i, val := range val.Elements {
			if val.Status == pgtype.Null {
				res[i] = null
			} else {
				res[i] = QuoteLiteral(val.String)
			}
		}

		return []byte("[" + strings.Join(res, ", ") + "]")
	}

	return nil
}

func QuoteLiteral(s string) string {
	return "'" + escaper.Replace(s) + "'"
}
