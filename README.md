# PostgreSQL to ClickHouse
[WIP]

Continuous data transfer from PostgreSQL to ClickHouse using logical replication mechanism

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
        buffer_table: {clickhouse buffer table name} # optional, if not specified, insert directly to the main table
        buffer_row_id: {clickhouse buffer table column name for row id} 
        skip_init_sync: {skip initial copy of the data}
        skip_buffer_table: {if true bypass buffer_table and write directly to the main_table}            
        engine: {clickhouse table engine: MergeTree, ReplacingMergeTree or CollapsingMergeTree}
        buffer_size: {number of DML(insert/update/delete) commands to store in the memory before flushing to the buffer/main table } 
        merge_threshold: {if buffer table specified, number of buffer flushed before moving data from buffer to the main table}
        columns: # postgres - clickhouse column name mapping, 
                 # if not present, all the columns are expected to be on the clickhouse side with the exact same names 
            {postgresql column name}: {clickhouse column name}
        empty_values: # in case of ReplacingMergeTree those values will be used to discard deleted rows
            {clickhouse column name}: {value to be used}
        sign_column: {clickhouse sign column name for MergeTree and CollapsingMergeTree engines only}
        ver_column: {clickhouse version column name for the ReplacingMergeTree engine}

inactivity_merge_timeout: {interval, default 1 min} # merge buffered data after that timeout
clickhouse:
    host: {clickhouse host, default 127.0.0.1}
    port: {tcp port, default 9000}
    database: {database name}
    username: {username}
    password: {password}
    params:
        {extra param name}:{extra param value}
        ...

pg: # postgresql connection params
    host: {host name}
    port: {port}
    database: {database name}
    user: {user}
    replication_slot_name: {logical replication slot name}
    publication_name: {postgresql publication name}
    
lsnStateFilepath: {state file to store applied LSN in}
```

### Sample setup:

- make sure you have PostgreSQL server running on `localhost:5432`
    - set `wal_level` in the postgresql config file to `logical`
    - set `max_replication_slots` to at least `1`
- make sure you have ClickHouse server running on `localhost:9000` e.g. in the [docker](https://hub.docker.com/r/yandex/clickhouse-server/)
- create database `pg2ch_test` in PostgreSQL: `CREATE DATABASE pg2ch_test;`
- create a set of tables using pgbench command: `pgbench -U postgres -d pg2ch_test -i`
- change [replica identity](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY)
for the `pgbench_accounts` table to FULL, so that we'll receive old values of the updated rows: `ALTER TABLE pgbench_accounts REPLICA IDENTITY FULL;`
- create PostgreSQL publication for the desired table(s): `CREATE PUBLICATION pg2ch_pub FOR TABLE pgbench_accounts;`
- create PostgreSQL logical replication slot: `SELECT * FROM pg_create_logical_replication_slot('pg2ch_slot', 'pgoutput');`
- create tables on the ClickHouse side:
```sql
CREATE TABLE pgbench_accounts (aid Int32, abalance Int32, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY aid
-- our target table

CREATE TABLE pgbench_accounts_buf (aid Int32, abalance Int32, sign Int8, row_id UInt64) ENGINE = Memory()
-- will be used as a buffer table
```
- create `config.yaml` file with the following content:
```yaml
tables:
    pgbench_accounts:
        main_table: pgbench_accounts
        buffer_table: pgbench_accounts_buf
        buffer_row_id: row_id
        engine: CollapsingMergeTree
        buffer_size: 1000
        merge_threshold: 4
        columns:
            aid: aid
            abalance: abalance
        sign_column: sign

inactivity_merge_timeout: '10s'

clickhouse:
    host: localhost
    port: 9000
    database: default
    username: default
pg:
    host: localhost
    port: 5432
    database: pg2ch_test
    user: postgres
    replication_slot_name: pg2ch_slot
    publication_name: pg2ch_pub
    
lsnStateFilepath: 'state.yaml'
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
    - ClickHouse: `SELECT SUM(abalance * sign) FROM pgbench_accounts` ([why multiply by `sign` column?](https://clickhouse.yandex/docs/en/operations/table_engines/collapsingmergetree/#example-of-use)) 
    - PostgreSQL: `SELECT SUM(abalance) FROM pgbench_accounts`
- the sums must match; if not, please open an issue.