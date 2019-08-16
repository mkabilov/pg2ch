package replicator

import (
	"fmt"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
)

func (r *Replicator) PreparePgTables() error {
	if err := r.pgDeltaConnect(); err != nil {
		return err
	}
	defer r.pgDeltaDisconnect()

	tx, err := r.pgDeltaConn.Begin()
	if err != nil {
		return err
	}

	pubTables := make([]config.PgTableName, 0)
	replIdentTables := make([]config.PgTableName, 0)
	for pgTableName := range r.cfg.Tables {
		var (
			curReplIdent  message.ReplicaIdentity
			inPublication bool
		)

		if err := tx.QueryRow("select relreplident from pg_class where oid = $1::regclass::oid", pgTableName.NamespacedName()).
			Scan(&curReplIdent); err != nil {
			return err
		}
		if curReplIdent != message.ReplicaIdentityFull {
			replIdentTables = append(replIdentTables, pgTableName)
		} else {
			r.logger.Infof("table %v has already %s replica identity", pgTableName, curReplIdent)
		}

		if err := tx.QueryRow("select $1::regclass::oid in (select relid from pg_get_publication_tables($2))",
			pgTableName.NamespacedName(), r.cfg.Postgres.PublicationName).Scan(&inPublication); err != nil {
			return err
		}
		if !inPublication {
			pubTables = append(pubTables, pgTableName)
		} else {
			r.logger.Infof("table %v is already in the publication", pgTableName)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	r.logger.Infof("need to set full replica identity for the %d tables", len(replIdentTables))
	if err := r.setReplicaIdentity(replIdentTables); err != nil {
		return err
	}

	r.logger.Infof("need to add %d tables to the %v publication", len(pubTables), r.cfg.Postgres.PublicationName)
	if err := r.addToPublication(pubTables); err != nil {
		return err
	}

	return nil
}

func (r *Replicator) setReplicaIdentity(pgTableNames []config.PgTableName) error {
	var (
		processedTables  int
		processedTblName config.PgTableName
	)

	for processedTables != len(pgTableNames) {
		for i, tableName := range pgTableNames {
			if tableName == processedTblName {
				continue
			}

			tx, err := r.pgDeltaConn.Begin()
			if err != nil {
				return fmt.Errorf("could not begin transaction: %v", err)
			}
			if _, err := tx.Exec("set statement_timeout = '5 s'"); err != nil {
				return fmt.Errorf("could not set statement timeout: %v", err)
			}

			_, err = tx.Exec(fmt.Sprintf("alter table %s replica identity full", tableName.NamespacedName()))
			if err == nil {
				r.logger.Infof("full replica identity is set for %v table", tableName)
				processedTables++
				pgTableNames[i] = processedTblName
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("could not commit tx: %v", err)
				}
			} else {
				r.logger.Infof("could not set replica identity for %v table: %v", tableName, err)
				if err := tx.Rollback(); err != nil {
					return fmt.Errorf("could not rollback tx: %v", err)
				}
			}
		}
	}

	return nil
}

func (r *Replicator) addToPublication(pgTableNames []config.PgTableName) error {
	var (
		processedTables  int
		processedTblName config.PgTableName
	)

	for processedTables != len(pgTableNames) {
		for i, tableName := range pgTableNames {
			if tableName == processedTblName {
				continue
			}

			tx, err := r.pgDeltaConn.Begin()
			if err != nil {
				return fmt.Errorf("could not begin transaction: %v", err)
			}
			if _, err := tx.Exec("set local statement_timeout = '5 s'"); err != nil {
				return fmt.Errorf("could not set statement timeout: %v", err)
			}

			_, err = tx.Exec(fmt.Sprintf("alter publication %s add table only %s",
				r.cfg.Postgres.PublicationName, tableName.NamespacedName()))
			if err == nil {
				r.logger.Infof("table %v added to %v publication", tableName, r.cfg.Postgres.PublicationName)
				processedTables++
				pgTableNames[i] = processedTblName
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("could not commit tx: %v", err)
				}
			} else {
				r.logger.Infof("could not add table %v to the publication: %v", tableName, err)
				if err := tx.Rollback(); err != nil {
					return fmt.Errorf("could not rollback tx: %v", err)
				}
			}
		}
	}

	return nil
}
