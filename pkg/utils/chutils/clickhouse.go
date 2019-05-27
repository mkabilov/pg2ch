package chutils

import (
	"fmt"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

var pgToChMap = map[string]string{
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
}

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
	}

	if pgColumn.IsArray {
		chType = fmt.Sprintf("Array(%s)", chType)
	}

	if pgColumn.IsNullable && !pgColumn.IsArray {
		chType = fmt.Sprintf("Nullable(%s)", chType)
	}

	return chType, nil
}
