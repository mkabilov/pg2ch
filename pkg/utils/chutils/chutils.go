package chutils

import (
	"fmt"
	"strings"

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

	timestampLength = 19 // ->2019-06-08 15:50:01<- clickhouse does not support milliseconds
)

var (
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

func ConvertColumn(colType string, val message.Tuple, colProps config.ColumnProperty) []byte {
	switch colType {
	case dbtypes.PgAdjustIstore:
		fallthrough
	case dbtypes.PgAdjustBigIstore:
		if colProps.FlattenIstore {
			if val.Kind == message.TupleNull {
				return []byte(strings.Repeat("\t\\N", colProps.FlattenIstoreMax-colProps.FlattenIstoreMin+1))[1:]
			}

			return pgutils.IstoreValues(val.Value, colProps.FlattenIstoreMin, colProps.FlattenIstoreMax)
		} else {
			if val.Kind == message.TupleNull {
				return istoreNull
			}

			return pgutils.IstoreToArrays(val.Value)
		}
	case dbtypes.PgAdjustAjBool:
		fallthrough
	case dbtypes.PgBoolean:
		if val.Kind == message.TupleNull {
			if len(colProps.Coalesce) > 0 {
				return colProps.Coalesce
			}
			return nullStr
		}

		switch val.Value[0] {
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
		if val.Kind == message.TupleNull {
			if len(colProps.Coalesce) > 0 {
				return colProps.Coalesce
			}
			return nullStr
		}

		return val.Value[:timestampLength]

	case dbtypes.PgTime:
		fallthrough
	case dbtypes.PgTimeWithoutTimeZone:
		//TODO
	case dbtypes.PgTimeWithTimeZone:
		//TODO
	}
	if val.Kind == message.TupleNull {
		if len(colProps.Coalesce) > 0 {
			return colProps.Coalesce
		}
		return nullStr
	}

	return val.Value
}
