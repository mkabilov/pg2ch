package tableinfo

import (
	"database/sql"
	"fmt"

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
