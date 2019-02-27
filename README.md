# PostgreSQL to Clickhouse
[WIP]

Data transfer from postgresql to clickhouse via logical replication mechanism

## Getting and running

Get:
```
    go get -u github.com/mkabilov/pg2ch
```

Run:
```
    pg2ch --config {path to the config file (default config.yaml)}
```


## Config file
```
tables:
    {postgresql table name}:
        main_table: {clickhouse table name} pgbench_accounts_repl
        buffer_table: {clickhouse buffer table name} # optional, if not specified, insert directly to the main table
        buffer_row_id: {clickhouse buffer table column name for row id} 
        inactivity_merge_timeout: {interval, default 1m } # merge buffered data after that timeout
        engine: {clickhouse table engine: MergeTree, ReplacingMergeTree or CollapsingMergeTree}
        buffer_size: {number of DML(insert/update/delete) commands to store in the memory before flushing to the buffer/main table } 
        merge_treshold: {if buffer table specified, number of buffer flushed before moving data from buffer to the main table}
        columns: # in the same order as in the postgresql table
            - {postgresql column name}:
                type: {Int8|Int16|Int32|Int64|UInt8|UInt16|UInt32|UInt64|Float32|Float64|String|DateTime}
                name: {clickhouse column name}
                empty_value: {value used for ReplacingMergeTree engine to discard deleted row value}
            - {postgresql column name}: # column will be ignored if no properties specified
        sign_column: {clickhouse sign column name for MergeTree and CollapsingMergeTree engines}
        ver_column: {clickhouse version column name for the ReplacingMergeTree engine}

clickhouse: {clickhouse connection string}
pg: # postgresql connection params
    host: {host name}
    port: {port}
    database: {database name}
    user: {user}
    replicationSlotName: {logical replication slot name}
    publicationName: {postgresql publication name}

```

sample:
```
tables:
    pgbench_accounts:
        main_table: pgbench_accounts_repl
        buffer_table: pgbench_accounts_buf
        buffer_row_id: 'buffer_command_id'
        inactivity_merge_timeout: '30s'
        engine: CollapsingMergeTree
        buffer_size: 1000
        merge_treshold: 4
        columns:
            - aid:
                type: Int32
                name: aid
            - abalance:
                type: Int32
                name: abalance
                empty_value: '0'
            - filler:
            - bid:
        sign_column: sign

clickhouse: tcp://localhost:9000
pg:
    host: localhost
    port: 5432
    database: pg2ch_test
    user: postgres
    replicationSlotName: myslot
    publicationName: my_pub
```
