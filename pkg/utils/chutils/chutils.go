package chutils

import (
	"fmt"
	"strings"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
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
		utils.PgSmallint:                 utils.ChInt16,
		utils.PgInteger:                  utils.ChInt32,
		utils.PgBigint:                   utils.ChInt64,
		utils.PgCharacterVarying:         utils.ChString,
		utils.PgVarchar:                  utils.ChString,
		utils.PgText:                     utils.ChString,
		utils.PgReal:                     utils.ChFloat32,
		utils.PgDoublePrecision:          utils.ChFloat64,
		utils.PgInterval:                 utils.ChInt32,
		utils.PgBoolean:                  utils.ChUInt8,
		utils.PgDecimal:                  utils.ChDecimal,
		utils.PgNumeric:                  utils.ChDecimal,
		utils.PgCharacter:                utils.ChFixedString,
		utils.PgChar:                     utils.ChFixedString,
		utils.PgJsonb:                    utils.ChString,
		utils.PgJson:                     utils.ChString,
		utils.PgUuid:                     utils.ChUUID,
		utils.PgBytea:                    utils.ChUInt8Array,
		utils.PgInet:                     utils.ChInt64,
		utils.PgTimestamp:                utils.ChDateTime,
		utils.PgTimestampWithTimeZone:    utils.ChDateTime,
		utils.PgTimestampWithoutTimeZone: utils.ChDateTime,
		utils.PgDate:                     utils.ChDate,
		utils.PgTime:                     utils.ChUint32,
		utils.PgTimeWithoutTimeZone:      utils.ChUint32,
		utils.PgTimeWithTimeZone:         utils.ChUint32,

		utils.PgAdjustIstore:    utils.ChInt32,    // type for istore values
		utils.PgAdjustBigIstore: utils.ChInt64,    // type for bigistore values
		utils.PgAdjustAjTime:    utils.ChDateTime, // adjust time
		utils.PgAdjustAjDate:    utils.ChDate,     // adjust date
		utils.PgAdjustAjBool:    utils.ChUInt8,    // adjust boolean: true, false, unknown
	}

	ajBoolUnkownValue = []byte("2")
	nullStr           = []byte(`\N`)
	istoreNull        = []byte("[]\t[]")
)

// ToClickHouseType converts pg type into clickhouse type
func ToClickHouseType(pgColumn config.PgColumn) (string, error) {
	chType, ok := pgToChMap[pgColumn.BaseType]
	if !ok {
		chType = utils.ChString
	}

	switch pgColumn.BaseType {
	case utils.PgDecimal:
		fallthrough
	case utils.PgNumeric:
		if pgColumn.Ext == nil {
			return "", fmt.Errorf("precision must be specified for the numeric type")
		}
		chType = fmt.Sprintf("%s(%d, %d)", chType, pgColumn.Ext[0], pgColumn.Ext[1])
	case utils.PgCharacter:
		fallthrough
	case utils.PgChar:
		if pgColumn.Ext == nil {
			return "", fmt.Errorf("length must be specified for character type")
		}
		chType = fmt.Sprintf("%s(%d)", chType, pgColumn.Ext[0])
	case utils.PgAdjustCountry:
		fallthrough
	case utils.PgAdjustOsName:
		fallthrough
	case utils.PgAdjustDeviceType:
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
	case utils.PgAdjustIstore:
		fallthrough
	case utils.PgAdjustBigIstore:
		if colProps.FlattenIstore {
			if val.Kind == message.TupleNull {
				return []byte(strings.Repeat("\t\\N", colProps.FlattenIstoreMax-colProps.FlattenIstoreMin+1))[1:]
			}

			return utils.IstoreValues(val.Value, colProps.FlattenIstoreMin, colProps.FlattenIstoreMax)
		} else {
			if val.Kind == message.TupleNull {
				return istoreNull
			}

			return utils.IstoreToArrays(val.Value)
		}
	case utils.PgAdjustAjBool:
		fallthrough
	case utils.PgBoolean:
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
	case utils.PgTimestampWithTimeZone:
		fallthrough
	case utils.PgTimestamp:
		if val.Kind == message.TupleNull {
			if len(colProps.Coalesce) > 0 {
				return colProps.Coalesce
			}
			return nullStr
		}

		return val.Value[:timestampLength]

	case utils.PgTime:
		fallthrough
	case utils.PgTimeWithoutTimeZone:
		//TODO
	case utils.PgTimeWithTimeZone:
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
