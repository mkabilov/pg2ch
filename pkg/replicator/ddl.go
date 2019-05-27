package replicator

import (
	"fmt"
	"strings"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
	"github.com/mkabilov/pg2ch/pkg/utils/tableinfo"
)

//GenerateChDDL generates clickhouse table DDLs
//TODO: refactor me
func (r *Replicator) GenerateChDDL() error {
	if err := r.pgConnect(); err != nil {
		return fmt.Errorf("could not connect to pg: %v", err)
	}

	tx, err := r.pgBegin()
	if err != nil {
		return fmt.Errorf("could not start transaction on pg side: %v", err)
	}

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
		for _, pgCol := range tblCfg.TupleColumns {
			chColName, ok := tblCfg.Columns[pgCol.Name]
			if !ok {
				continue
			}

			pgCol := tblCfg.PgColumns[pgCol.Name]
			chColDDL, err := chutils.ToClickHouseType(pgCol)
			if err != nil {
				return fmt.Errorf("could not get clickhouse column definition: %v", err)
			}
			if pgCol.PkCol > 0 && pgCol.PkCol > pkColumnNumb {
				pkColumnNumb = pgCol.PkCol
			}

			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s %s", chColName, chColDDL))
		}
		pkColumns := make([]string, pkColumnNumb)

		for pgColName := range tblCfg.Columns {
			pgCol := tblCfg.PgColumns[pgColName]
			if pgCol.PkCol < 1 {
				continue
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
			tblCfg.Engine.String(), engineParams)

		if len(pkColumns) > 0 {
			orderBy = fmt.Sprintf(" ORDER BY(%s)", strings.Join(pkColumns, ", "))
		}
		tableDDL += orderBy + ";"

		fmt.Println(tableDDL)

		if tblCfg.ChBufferTable != "" {
			fmt.Println(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n) Engine = MergeTree()%s;",
				tblCfg.ChBufferTable,
				strings.Join(
					append(chColumnDDLs, fmt.Sprintf("    %s UInt64", tblCfg.BufferTableRowIdColumn)), ",\n"),
				orderBy))
		}

	}

	return nil
}
