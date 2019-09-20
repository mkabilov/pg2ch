[![Build Status](https://travis-ci.org/mkabilov/pg2ch.svg?branch=http)](https://travis-ci.org/mkabilov/pg2ch)

# PostgreSQL to ClickHouse

Continuous data transfer from PostgreSQL to ClickHouse using logical replication mechanism.

### Status of the project
Currently pg2ch tool is in active testing stage,
as for now it is not for production use

### Getting and running

Get:
```
    go get -u github.com/mkabilov/pg2ch
```

Run:
```
    pg2ch --config {path to the config file (default config.yaml)}
```


### Config file
```yaml
tables:
    {postgresql table name}:
        main_table: {clickhouse table name}
        sync_aux_table: {clickhouse auxilary table, used for storing incoming changes of table which is in sync process}
        row_id_column: {name of the row_id column in the aux table, default "row_id"}
        table_name_column_name: {name of the column in the aux table which stores the name of the postgresql table the data came from, default "table_name"}
        lsn_column_name: {name of the column in the aux and main tables which is used to store the origin lsn of the row, default "lsn"}
        init_sync_skip: {skip initial copy of the data}
        init_sync_skip_truncate: {skip truncate of the main_table during init sync}
        engine: {clickhouse table engine: MergeTree, ReplacingMergeTree or CollapsingMergeTree}
        max_buffer_length: {number of DML(insert/update/delete) commands to store in the memory before flushing to the main table } 
        columns: # postgres - clickhouse column name mapping, 
                 # if not present, all the columns are expected to be on the clickhouse side with the exact same names 
            {postgresql column name}: {clickhouse column name}
        column_properties:
                {postgresql column name}:
                        istore_keys_suffix: {prefix for the istore keys column}
                        istore_values_suffix: {prefix for the istore values column}
                        coalesce: {in case of pg column has null value, replace it with value entered here}
        is_deleted_column: {in case of ReplacingMergeTree 1 will be stored in the {is_deleted_column} in order to mark deleted rows}
        sign_column: {clickhouse sign column name for CollapsingMergeTree engines only, default "sign"}

inactivity_merge_timeout: {interval, default 1 min} # merge buffered data after that timeout

clickhouse: # clickhouse tcp protocol connection params
    host: {clickhouse host, default 127.0.0.1}
    port: {tcp port, default 8123}
    database: {database name}
    username: {username}
    password: {password}
    params:
        {extra param name}:{extra param value}
        ...

postgres: # postgresql connection params
    host: {host name, default 127.0.0.1}
    port: {port, default 5432}
    database: {database name}
    user: {user}
    replication_slot_name: {logical replication slot name}
    publication_name: {postgresql publication name}
    
db_path: {path to the persistent storage dir where table lsn positions will be stored}
db_type: {type of the storage, "diskv" or "mmap", default "diskv"}
gzip_compression: {compression level: "no", "bestspeed", "bestcompression", "default", "huffmanonly", default: "no"}
```

### Sample setup:

- make sure you have PostgreSQL server running on `localhost:5432`
    - set `wal_level` in the postgresql config file to `logical`
    - set `max_replication_slots` to at least `2`
- make sure you have ClickHouse server running on `localhost:8123` e.g. in the [docker](https://hub.docker.com/r/yandex/clickhouse-server/)
- create database `pg2ch_test` in PostgreSQL: `CREATE DATABASE pg2ch_test;`
- create a set of tables using pgbench command: `pgbench -U postgres -d pg2ch_test -i`
- change [replica identity](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY)
for the `pgbench_accounts` table to FULL, so that we'll receive old values of the updated rows: `ALTER TABLE pgbench_accounts REPLICA IDENTITY FULL;`
- create PostgreSQL publication for the `pgbench_accounts` table: `CREATE PUBLICATION pg2ch_pub FOR TABLE pgbench_accounts;`
- create PostgreSQL logical replication slot: `SELECT * FROM pg_create_logical_replication_slot('pg2ch_slot', 'pgoutput');`
- create tables on the ClickHouse side:
```sql
CREATE TABLE pgbench_accounts (aid Int32, abalance Int32, sign Int8, lsn UInt64) ENGINE = CollapsingMergeTree(sign) ORDER BY aid;
-- our target table

CREATE TABLE pgbench_accounts_aux (aid Int32, abalance Int32, sign Int8, row_id UInt64, lsn UInt64, table_name String) ENGINE = Memory();
-- will be used as an aux table
```
- create `config.yaml` file with the following content:
```yaml
tables:
    pgbench_accounts:
        main_table: pgbench_accounts
        sync_aux_table: pgbench_accounts_aux
        engine: CollapsingMergeTree
        max_buffer_length: 1000
        columns:
            aid: aid
            abalance: abalance
        sign_column: sign

inactivity_merge_timeout: '10s'

clickhouse:
    host: localhost
    database: default
    username: default
postgres:
    host: localhost
    database: pg2ch_test
    user: postgres
    replication_slot_name: pg2ch_slot
    publication_name: pg2ch_pub
    
db_path: db
gzip_compression: bestspeed
sync_workers: 1
```

- run pg2ch to start replication:
```bash
    pg2ch --config config.yaml
```

- run `pgbench` to have some test load:
```bash
    pgbench -U postgres -d pg2ch_test --time 30 --client 10 
```
- wait for `inactivity_merge_timeout` period (in our case 10 seconds) so that data in the memory gets flushed to the table in ClickHouse
- check the sums of the `abalance` column both on ClickHouse and PostgreSQL:
    - ClickHouse: `SELECT SUM(abalance * sign), SUM(sign) FROM pgbench_accounts` ([why multiply by `sign` column?](https://clickhouse.yandex/docs/en/operations/table_engines/collapsingmergetree/#example-of-use)) 
    - PostgreSQL: `SELECT SUM(abalance), COUNT(*) FROM pgbench_accounts`
- numbers must match; if not, please open an issue.
