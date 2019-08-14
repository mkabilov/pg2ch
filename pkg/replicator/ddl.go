package replicator

import (
	"fmt"
	"strings"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/tableinfo"
)

var orderByCols = map[string]string{}

//GenerateChDDL generates clickhouse table DDLs
//TODO: refactor me
func (r *Replicator) GenerateChDDL() error {
	if err := r.pgConnect(); err != nil {
		return fmt.Errorf("could not connect to pg: %v", err)
	}

	tx, err := r.pgBegin(r.pgDeltaConn)
	if err != nil {
		return fmt.Errorf("could not start transaction on pg side: %v", err)
	}
	processedTables := make(map[config.ChTableName]struct{})

	for tblName := range r.cfg.Tables {
		var (
			pkColumnNumb int
			engineParams string
			orderBy      string
		)

		tblCfg := r.cfg.Tables[tblName]

		tblCfg.TupleColumns, tblCfg.PgColumns, err = tableinfo.TablePgColumns(tx, tblName)
		if err != nil {
			return fmt.Errorf("could not get columns for %s postgres table: %v", tblName.String(), err)
		}

		if len(tblCfg.Columns) == 0 {
			tblCfg.Columns = make(map[string]string)
			for _, pgCol := range tblCfg.TupleColumns {
				tblCfg.Columns[pgCol.Name] = pgCol.Name
			}
		}

		chColumnDDLs := make([]string, 0)
		for _, tupleColumn := range tblCfg.TupleColumns {
			chColName, ok := tblCfg.Columns[tupleColumn.Name]
			if !ok {
				continue
			}

			pgCol := tblCfg.PgColumns[tupleColumn.Name]
			chColType, err := chutils.ToClickHouseType(pgCol)
			if err != nil {
				return fmt.Errorf("could not get clickhouse column definition: %v", err)
			}

			if pgCol.PkCol > 0 && pgCol.PkCol > pkColumnNumb {
				pkColumnNumb = pgCol.PkCol
			}
			columnCfg, hasColumnCfg := tblCfg.ColumnProperties[tupleColumn.Name]
			if !hasColumnCfg && (pgCol.BaseType == dbtypes.PgAdjustIstore || pgCol.BaseType == dbtypes.PgAdjustBigIstore) {
				hasColumnCfg = true
				columnCfg = config.ColumnProperty{
					IstoreKeysSuffix:   "keys",
					IstoreValuesSuffix: "values",
				}
			}

			if hasColumnCfg {
				switch {
				case columnCfg.IstoreKeysSuffix != "":
					chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s_%s Array(Int32)", chColName, columnCfg.IstoreKeysSuffix))
					if pgCol.BaseType == dbtypes.PgAdjustBigIstore {
						chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s_%s Array(Int64)", chColName, columnCfg.IstoreValuesSuffix))
					} else {
						chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s_%s Array(Int32)", chColName, columnCfg.IstoreValuesSuffix))
					}
				default:
					chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s %s", chColName, chColType))
				}
				continue
			}

			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s %s", chColName, chColType))
		}
		pkColumns := make([]string, pkColumnNumb)

		var partitionColumn string
		for pgColName := range tblCfg.Columns {
			pgCol := tblCfg.PgColumns[pgColName]
			if pgCol.PkCol < 1 {
				continue
			}

			if pgCol.IsTime() && partitionColumn == "" {
				partitionColumn = pgColName
			}

			pkColumns[pgCol.PkCol-1] = pgColName
		}

		if tblCfg.GenerationColumn != "" {
			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s UInt32", tblCfg.GenerationColumn))
		}

		switch tblCfg.Engine {
		case config.ReplacingMergeTree:
			if tblCfg.VerColumn != "" {
				chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s UInt64", tblCfg.VerColumn))
				engineParams = tblCfg.VerColumn
			} else {
				engineParams = tblCfg.GenerationColumn
			}

			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s UInt8", tblCfg.IsDeletedColumn))
		case config.CollapsingMergeTree:
			engineParams = tblCfg.SignColumn
			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s Int8", engineParams))
		}

		tableDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n) Engine = %s(%s)",
			tblCfg.ChMainTable,
			strings.Join(chColumnDDLs, ",\n"),
			tblCfg.Engine.String(),
			engineParams)
		if partitionColumn != "" {
			tableDDL += fmt.Sprintf(" PARTITION BY toStartOfMonth(%s)", partitionColumn)
		}

		if ob, ok := orderByCols[tblCfg.ChMainTable.String()]; ok {
			orderBy = ob
		} else if len(pkColumns) > 0 {
			orderBy = fmt.Sprintf(" ORDER BY(%s)", strings.Join(pkColumns, ", "))
		}
		tableDDL += orderBy + ";"

		if _, ok := processedTables[tblCfg.ChMainTable]; !ok {
			fmt.Println(tableDDL)
			processedTables[tblCfg.ChMainTable] = struct{}{}
		}

		if !tblCfg.ChBufferTable.IsEmpty() {
			if _, ok := processedTables[tblCfg.ChBufferTable]; !ok {
				fmt.Printf("CREATE TABLE IF NOT EXISTS %s (\n%s\n) Engine = MergeTree()%s;\n",
					tblCfg.ChBufferTable,
					strings.Join(
						append(chColumnDDLs, fmt.Sprintf("    %s UInt64", tblCfg.BufferTableRowIdColumn)), ",\n"),
					orderBy)
				processedTables[tblCfg.ChBufferTable] = struct{}{}
			}
		}

		if !tblCfg.ChSyncAuxTable.IsEmpty() {
			if _, ok := processedTables[tblCfg.ChSyncAuxTable]; !ok {
				fmt.Printf("CREATE TABLE IF NOT EXISTS %s (\n%s\n) Engine = MergeTree()%s;\n",
					tblCfg.ChSyncAuxTable,
					strings.Join(
						append(chColumnDDLs,
							fmt.Sprintf("    %s UInt64", tblCfg.BufferTableRowIdColumn),
							fmt.Sprintf("    %s UInt64", tblCfg.LsnColumnName),
						), ",\n"),
					orderBy)
				processedTables[tblCfg.ChSyncAuxTable] = struct{}{}
			}
		}

	}

	return nil
}
