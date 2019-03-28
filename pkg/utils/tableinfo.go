package utils

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
)

func TableChColumns(chConn *sql.DB, databaseName, chTableName string) (map[string]config.ChColumn, error) {
	result := make(map[string]config.ChColumn)

	rows, err := chConn.Query("select name, type from system.columns where database = ? and table = ?",
		databaseName, chTableName)

	if err != nil {
		return nil, fmt.Errorf("could not query: %v", err)
	}

	for rows.Next() {
		var colName, colType string

		if err := rows.Scan(&colName, &colType); err != nil {
			return nil, fmt.Errorf("could not scan: %v", err)
		}

		result[colName] = config.ChColumn{
			Name:   colName,
			Column: parseChType(colType),
		}
	}

	return result, nil
}

// TablePgColumns returns postgresql table's columns structure
func TablePgColumns(tx *pgx.Tx, tblName config.PgTableName) ([]string, map[string]config.PgColumn, error) {
	columns := make([]string, 0)
	pgColumns := make(map[string]config.PgColumn)

	rows, err := tx.Query(`select
  a.attname,
  not a.attnotnull,
  a.atttypid::regtype::text,
  string_to_array(substring(format_type(a.atttypid, a.atttypmod) from '\((.*)\)'), ',') as ext,
  coalesce(ai.attnum, 0) as pk_attnum
from pg_class c
  inner join pg_namespace n on n.oid = c.relnamespace
  inner join pg_attribute a on a.attrelid = c.oid
  left join pg_index i on i.indrelid = a.attrelid and i.indisprimary
  left join pg_attribute ai on ai.attrelid = i.indexrelid and ai.attname = a.attname and ai.attisdropped = false
where
  n.nspname = $1
  and c.relname = $2
  and a.attnum > 0
  and a.attisdropped = false
order by
  a.attnum`, tblName.SchemaName, tblName.TableName)

	if err != nil {
		return nil, nil, fmt.Errorf("could not query: %v", err)
	}

	for rows.Next() {
		var (
			colName, baseType string
			pgColumn          config.PgColumn
			extStr            []string
		)

		if err := rows.Scan(&colName, &pgColumn.IsNullable, &baseType, &extStr, &pgColumn.PkCol); err != nil {
			return nil, nil, fmt.Errorf("could not scan: %v", err)
		}

		if baseType[len(baseType)-2:] == "[]" {
			pgColumn.IsArray = true
			pgColumn.BaseType = baseType[:len(baseType)-2]
		} else {
			pgColumn.BaseType = baseType
		}

		if extStr != nil {
			pgColumn.Ext, err = strToIntArray(extStr)
			if err != nil {
				return nil, nil, fmt.Errorf("could not convert into int array: %v", err)
			}
		}

		columns = append(columns, colName)
		pgColumns[colName] = pgColumn
	}

	return columns, pgColumns, nil
}

func strToIntArray(str []string) ([]int, error) {
	var err error
	ints := make([]int, len(str))
	for i, strVal := range str {
		ints[i], err = strconv.Atoi(strVal)
		if err != nil {
			return nil, err
		}
	}

	return ints, nil
}

func parseChType(chType string) (col config.Column) {
	col = config.Column{BaseType: chType, IsArray: false, IsNullable: false}

	if ln := len(chType); ln >= 7 {
		if strings.HasPrefix(chType, "Array(Nullable(") {
			col = config.Column{BaseType: chType[15 : ln-2], IsArray: true, IsNullable: true}
		} else if strings.HasPrefix(chType, "Array(") {
			col = config.Column{BaseType: chType[6 : ln-1], IsArray: true, IsNullable: false}
		} else if strings.HasPrefix(chType, "Nullable(") {
			col = config.Column{BaseType: chType[9 : ln-1], IsArray: false, IsNullable: true}
		}
	}

	if strings.HasPrefix(col.BaseType, "FixedString(") {
		col.BaseType = "FixedString"
	}

	return
}
