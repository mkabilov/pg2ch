package tableinfo

import (
	"fmt"

	"github.com/mkabilov/pg2ch/pkg/utils/chutils/loader"

	"github.com/mkabilov/pg2ch/pkg/config"
)

func TableChColumns(chConnectionString, databaseName string, chTableName string) (map[string]config.ChColumn, error) {
	result := make(map[string]config.ChColumn)

	chLoader := loader.New(chConnectionString, databaseName)

	rows, err := chLoader.Query(fmt.Sprintf("select name, type from system.columns where database = '%s' and table = '%s'",
		databaseName, chTableName)) //TODO: fix SQL injections
	if err != nil {
		return nil, fmt.Errorf("could not query: %v", err)
	}

	for _, line := range rows {
		result[line[0]] = config.ChColumn{
			Name:   line[0],
			Column: parseChType(line[1]),
		}
	}

	return result, nil
}
