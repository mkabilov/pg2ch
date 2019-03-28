package utils

import (
	"fmt"

	"github.com/mkabilov/pg2ch/pkg/config"
)

var pgToChMap = map[string]string{
	"smallint":                    "Int16",
	"integer":                     "Int32",
	"bigint":                      "Int64",
	"character varying":           "String",
	"varchar":                     "String",
	"text":                        "String",
	"real":                        "Float32",
	"double precision":            "Float64",
	"timestamp":                   "DateTime",
	"timestamp with time zone":    "DateTime",
	"timestamp without time zone": "DateTime",
	"date":                        "Date",
	"time":                        "Uint32",
	"time without time zone":      "Uint32",
	"time with time zone":         "Uint32",
	"interval":                    "Int32",
	"boolean":                     "UInt8",
	"decimal":                     "Decimal",
	"numeric":                     "Decimal",
	"character":                   "FixedString",
	"char":                        "FixedString",
	"jsonb":                       "String",
	"uuid":                        "UUID",
	"bytea":                       "Array(UInt8)",
	"inet":                        "bigint",
}

// ToClickHouseType converts pg type into clickhouse type
func ToClickHouseType(pgColumn config.PgColumn) (string, error) {
	chType, ok := pgToChMap[pgColumn.BaseType]
	if !ok {
		return "", fmt.Errorf("could not convert type %s", pgColumn.BaseType)
	}

	switch pgColumn.BaseType {
	case "decimal":
		fallthrough
	case "numeric":
		if pgColumn.Ext == nil {
			return "", fmt.Errorf("precision must be specified for the numeric type")
		}
		chType = fmt.Sprintf("%s(%d, %d)", chType, pgColumn.Ext[0], pgColumn.Ext[1])
	case "character":
		fallthrough
	case "char":
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
