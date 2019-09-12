package replicator

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/tableinfo"
)

const (
	pkColumnsFile         = "pk_columns.yaml"
	compressedColumnsFile = "compress_columns.yaml"
	codecSuffix           = " CODEC(Delta, LZ4)"
)

var (
	orderByCols     = make(map[string][]string)
	compressColumns = map[string]struct{}{}
)

//GenerateChDDL generates ClickHouse table DDLs
//TODO: refactor me
func (r *Replicator) GenerateChDDL() error {
	if _, err := os.Stat(pkColumnsFile); err == nil {
		fp, err := os.Open(pkColumnsFile)
		if err == nil {
			if err := yaml.NewDecoder(fp).Decode(orderByCols); err != nil {
				return fmt.Errorf("could not read primary keys file: %v", err)
			}
			fp.Close()
		}
	}

	if _, err := os.Stat(compressedColumnsFile); err == nil {
		fp, err := os.Open(compressedColumnsFile)
		if err == nil {
			if err := yaml.NewDecoder(fp).Decode(compressColumns); err != nil {
				return fmt.Errorf("could not read primary keys file: %v", err)
			}
			fp.Close()
		}
	}

	for key, val := range orderByCols {
		newKey := r.cfg.ClickHouse.Database + "." + key + "_main"
		delete(orderByCols, key)
		orderByCols[newKey] = val
	}

	if err := r.pgDeltaConnect(); err != nil {
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
			columnCfg, hasColumnCfg := tblCfg.ColumnProperties[tupleColumn.Name]

			pgCol := tblCfg.PgColumns[tupleColumn.Name]
			if hasColumnCfg && len(columnCfg.Coalesce) > 0 {
				pgCol.IsNullable = false
			}

			chColType, err := chutils.ToClickHouseType(pgCol)
			if err != nil {
				return fmt.Errorf("could not get clickhouse column definition: %v", err)
			}

			if pgCol.PkCol > 0 && pgCol.PkCol > pkColumnNumb {
				pkColumnNumb = pgCol.PkCol
			}
			if !hasColumnCfg && (pgCol.BaseType == dbtypes.PgAdjustIstore || pgCol.BaseType == dbtypes.PgAdjustBigIstore) {
				hasColumnCfg = true
				columnCfg = config.ColumnProperty{
					IstoreKeysSuffix:   "keys",
					IstoreValuesSuffix: "values",
				}
			}

			if hasColumnCfg && columnCfg.IstoreKeysSuffix != "" {
				chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("%s_%s Array(Int32)", chColName, columnCfg.IstoreKeysSuffix))

				if pgCol.BaseType == dbtypes.PgAdjustBigIstore {
					chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("%s_%s Array(Int64)", chColName, columnCfg.IstoreValuesSuffix))
				} else {
					chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("%s_%s Array(Int32)", chColName, columnCfg.IstoreValuesSuffix))
				}
				continue
			}
			codec := ""
			if _, ok := compressColumns[chColName]; ok {
				codec = codecSuffix
			}

			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("%s %s%s", chColName, chColType, codec))
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

		if tblCfg.LsnColumnName != "" {
			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("%s UInt64 CODEC(Delta, LZ4)", tblCfg.LsnColumnName))
		}

		switch tblCfg.Engine {
		case config.ReplacingMergeTree:
			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("%s UInt8", tblCfg.IsDeletedColumn))
		case config.CollapsingMergeTree:
			engineParams = tblCfg.SignColumn
			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("%s Int8", engineParams))
		}

		tableDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) Engine = %s(%s)",
			tblCfg.ChMainTable,
			strings.Join(chColumnDDLs, ", "),
			tblCfg.Engine.String(),
			engineParams)
		if partitionColumn != "" {
			tableDDL += fmt.Sprintf(" PARTITION BY toStartOfMonth(%s)", partitionColumn)
		}

		if ob, ok := orderByCols[tblCfg.ChMainTable.String()]; ok {
			orderBy = fmt.Sprintf(" ORDER BY(%s)", strings.Join(ob, ", "))
		} else if len(pkColumns) > 0 {
			orderBy = fmt.Sprintf(" ORDER BY(%s)", strings.Join(pkColumns, ", "))
		}
		tableDDL += orderBy + ";"

		if _, ok := processedTables[tblCfg.ChMainTable]; !ok {
			fmt.Println(tableDDL)
			processedTables[tblCfg.ChMainTable] = struct{}{}
		}

		if !tblCfg.ChSyncAuxTable.IsEmpty() {
			if _, ok := processedTables[tblCfg.ChSyncAuxTable]; !ok {
				columns := append(chColumnDDLs, fmt.Sprintf("%s UInt64", tblCfg.RowIDColumnName))
				columns = append(columns, fmt.Sprintf("%s String", tblCfg.TableNameColumnName))

				fmt.Printf("CREATE TABLE IF NOT EXISTS %s (%s) Engine = MergeTree() ORDER BY (%s) PARTITION BY (%s);\n",
					tblCfg.ChSyncAuxTable,
					strings.Join(columns, ", "),
					tblCfg.LsnColumnName,
					tblCfg.TableNameColumnName)
				processedTables[tblCfg.ChSyncAuxTable] = struct{}{}
			}
		}
	}

	return nil
}
