package main

import (
	"fmt"
	"github.com/jackc/pgx"
	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
	"github.com/mkabilov/pg2ch/pkg/utils/tableinfo"
	"log"
	"os"
)

func main() {
	pgxCfg, err := pgx.ParseEnvLibpq()
	if err != nil {
		log.Fatalf("could not parse env vars %v", err)
	}

	db, err := pgx.Connect(pgxCfg)
	if err != nil {
		log.Fatalf("could not connect to db: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("could not start transaction: %v", err)
	}

	tbl := config.PgTableName{"public", "my_table"}

	ddl, err := generateChDDL(tx, tbl)
	if err != nil {
		log.Printf("could not fetch table info: %v", err)
	}
	fmt.Println(ddl)
}

func generateChDDL(tx *pgx.Tx, tblName config.PgTableName) (string, error) {
	tupleColumns, pgColumns, err := tableinfo.TablePgColumns(tx, tblName)
	if err != nil {
		return "", fmt.Errorf("could not get columns for %s postgres table: %v", tblName.String(), err)
	}

	chColumnDDLs := make([]string, 0)
	for _, tplCol := range tupleColumns {
		var chColDDL string
		pgCol := pgColumns[tplCol.Name]

		if pgCol.BaseType == utils.PgJsonb {

			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s %s", tplCol.Name, chColDDL))
			continue
		}
		chColDDL, err = chutils.ToClickHouseType(pgCol)
		if err != nil {
			return "", fmt.Errorf("could not get clickhouse column definition: %v", err)
		}

		chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s %s", tplCol.Name, chColDDL))
	}

	log.Printf("%v", chColumnDDLs)
	os.Exit(0)

	return "", nil
}
