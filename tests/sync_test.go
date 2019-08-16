package tests

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ildus/pqt"
	"github.com/stretchr/testify/assert"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/replicator"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

const (
	initpg = `
do $$
begin
	if not exists (select from pg_catalog.pg_roles
		where rolname = 'postgres')
	then
		create role postgres superuser login;
	end if;
end $$;
create extension istore;
create table pg1(id bigserial, a int, b smallint, c bigint, d text, f1 float,
	f2 double precision, bo bool, num numeric(10, 2), ch varchar(10));
alter table pg1 replica identity full;
create table pg2(id bigserial, a int[], b bigint[], c text[]);
alter table pg2 replica identity full;
create table pg3(id bigserial, a istore, b bigistore);
alter table pg3 replica identity full;
insert into pg1(a,b,c,d,f1,f2,bo,num,ch) select i, i + 1, i + 2, i::text, i + 1.1,
	i + 2.1, true, i + 3, (i+4)::text
from generate_series(1, 10000) i;
insert into pg2(a, b, c) select array_fill(i, array[3]), array_fill(i + 1, array[3]),
	array_fill(i::text, array[3])
from generate_series(1, 10000) i;
insert into pg3(a, b) select
	istore(array_fill(i, array[3]), array_fill(i + 1, array[3])),
	bigistore(array_fill(i + 2, array[3]), array_fill(i + 3, array[3]))
from generate_series(1, 10000) i;
`
	addsql = `
insert into pg1(a,b,c,d,f1,f2,bo,num,ch) select i, i + 1, i + 2, i::text,
	i + 1.1, i + 2.1, true, i + 3, (i+4)::text from generate_series(1, 100) i;
insert into pg2(a, b, c) select array_fill(i, array[3]), array_fill(i + 1, array[3]),
	array_fill(i::text, array[3]) from generate_series(1, 100) i;
insert into pg3(a, b) select
	istore(array_fill(i, array[1]), array_fill(i + 1, array[1])),
	bigistore(array_fill(i + 2, array[1]), array_fill(i + 3, array[1]))
from generate_series(1, 100) i;
`
	testConfigFile = "./test.yaml"
)

type CHLink struct {
	conn *chutils.CHConn
}

var (
	ch     CHLink
	initch = []string{
		"drop database if exists pg2ch_test;",
		"create database pg2ch_test;",
		`create table pg2ch_test.ch1(
			id UInt64,
			a Int32,
			b Int8,
			c Int64,
			d String,
			f1 Float32,
			f2 Float64,
			bo Int8,
			num Decimal(10, 2),
			ch String,
			sign Int8
		 ) engine=CollapsingMergeTree(sign) order by id;`,
		`create table pg2ch_test.ch1_aux(
			id UInt64,
			a Int32,
			b Int8,
			c Int64,
			d String,
			f1 Float32,
			f2 Float64,
			bo Int8,
			num Decimal(10, 2),
			ch String,
			sign Int8,
			row_id UInt64,
			lsn UInt64
		) engine=CollapsingMergeTree(sign) order by id;`,
		`create table pg2ch_test.ch2(
			id UInt64,
			a Array(Int32),
			b Array(Int64),
			c Array(String),
			sign Int8
		 ) engine=CollapsingMergeTree(sign) order by id;`,
		`create table pg2ch_test.ch2_aux(
			id UInt64,
			a Array(Int32),
			b Array(Int64),
			c Array(String),
			sign Int8,
			row_id UInt64,
			lsn UInt64
		) engine=CollapsingMergeTree(sign) order by id;`,
		`create table pg2ch_test.ch3(
			id UInt64,
			a_keys Array(Int32),
			a_values Array(Int32),
			b_keys Array(Int32),
			b_values Array(Int64),
			sign Int8
		 ) engine=CollapsingMergeTree(sign) order by id;`,
		`create table pg2ch_test.ch3_aux(
			id UInt64,
			a_keys Array(Int32),
			a_values Array(Int32),
			b_keys Array(Int32),
			b_values Array(Int64),
			sign Int8,
			row_id UInt64,
			lsn UInt64
		) engine=CollapsingMergeTree(sign) order by id;`,
	}
)

func (ch *CHLink) safeExec(t *testing.T, sql string) {
	err := ch.conn.Exec(sql)
	if err != nil {
		t.Fatal("could not exec query:", err)
	}
}

func (ch *CHLink) safeQuery(t *testing.T, sql string) [][]string {
	rows, err := ch.conn.Query(sql)
	if err != nil {
		t.Fatal("could not make query:", err)
	}
	return rows
}

func (ch *CHLink) waitForCount(t *testing.T, query string, min_count int, timeout int) {
	counter := 0

	for {
		rows := ch.safeQuery(t, query)
		recCount, err := strconv.Atoi(rows[0][0])
		if err != nil {
			t.Fatal(err)
		}

		if recCount >= min_count {
			break
		}

		counter += 1
		time.Sleep(time.Second)

		if counter >= timeout {
			t.Fatal("timeout on query: ", query)
		}
	}

}

func (ch *CHLink) getCount(t *testing.T, query string) int {
	rows := ch.safeQuery(t, query)
	recCount, err := strconv.Atoi(rows[0][0])
	if err != nil {
		t.Fatalf("count not get count for: %s : %s", query, err)
	}

	return recCount
}

func TestBasicSync(t *testing.T) {
	node := pqt.MakePostgresNode("master")
	defer node.Stop()

	config.DefaultPostgresPort = uint16(node.Port)
	cfg, err := config.New(testConfigFile)
	if cfg.PersStorageType == "diskv" {
		db_path, err := ioutil.TempDir("", "pg2ch_diskv_dat")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(db_path)

		cfg.PersStoragePath = db_path
	} else if cfg.PersStorageType == "mmap" {
		tmpfile, err := ioutil.TempFile("", "pg2ch_mmap_dat")
		fmt.Println(tmpfile.Name())
		if err != nil {
			t.Fatal(err)
		}
		cfg.PersStoragePath = tmpfile.Name()
	} else {
		t.Fatal("unknown db type")
	}
	ch.conn = chutils.MakeChConnection(&cfg.ClickHouse)

	if err != nil {
		log.Fatal("config parsing error: ", err)
	}

	node.Init()
	node.AppendConf("postgresql.conf", `
log_min_messages = ERROR
log_statement = none
hot_standby = on
wal_keep_segments = 10
wal_level = logical
max_logical_replication_workers = 10
`)

	node.AppendConf("pg_hba.conf", `
	   local	all		all						trust
	   host	all		all		127.0.0.1/32	trust
	   host	all		all		::1/128			trust
	   local	replication		all						trust
	   host	replication		all		127.0.0.1/32	trust
	   host	replication		all		::1/128			trust
	`)
	node.Start()
	node.Execute("postgres", initpg)
	node.Execute("postgres", fmt.Sprintf("create publication %s for all tables",
		cfg.Postgres.PublicationName))
	node.Execute("postgres", fmt.Sprintf("select pg_create_logical_replication_slot('%s', 'pgoutput')",
		cfg.Postgres.ReplicationSlotName))

	for _, s := range initch {
		ch.safeExec(t, s)
	}

	var repl *replicator.Replicator

	stopCh := make(chan bool, 1)
	go func() {
		repl = replicator.New(cfg)
		err := repl.Run()
		if err != nil {
			stopCh <- true
			t.Fatal("could not start replicator: ", err)
		}
		stopCh <- true
	}()

	go func() {
		ch.waitForCount(t, "select count(*) from pg2ch_test.ch1", 1, 10)
		if ch.getCount(t, "select count(*) from pg2ch_test.ch1") != 10000 {
			t.Fatal("count shoud be equal to 10000")
		}

		for i := 0; i < 100; i++ {
			node.Execute("postgres", addsql)
		}
		ch.waitForCount(t, "select count(*) from pg2ch_test.ch1", 20000, 10)
		count := ch.getCount(t, "select count(*) from pg2ch_test.ch1")
		if count != 20000 {
			t.Fatal("count shoud be equal to 20000")
		}

		count = ch.getCount(t, "select count(*) from pg2ch_test.ch2")
		if count != 20000 {
			t.Fatal("count shoud be equal to 20000")
		}

		count = ch.getCount(t, "select count(*) from pg2ch_test.ch3")
		if count != 20000 {
			t.Fatal("count shoud be equal to 20000")
		}

		rows := ch.safeQuery(t, "select * from pg2ch_test.ch1 order by id desc limit 10")
		assert.Equal(t, rows[0], []string{"20000", "100", "101", "102", "100", "101.1", "102.1", "1", "103.00", "104", "1"}, "row 0")

		rows = ch.safeQuery(t, "select * from pg2ch_test.ch2 order by id desc limit 10")
		assert.Equal(t, rows[0][0], "20000", "row 0")
		assert.Equal(t, rows[0][1], "[100,100,100]", "row 0")
		assert.Equal(t, rows[0][2], "[101,101,101]", "row 0")
		assert.Equal(t, rows[0][3], "['100','100','100']", "row 0")
		assert.Equal(t, rows[0][4], "1", "row 0")

		assert.Equal(t, rows[1][0], "19999", "row 1")
		assert.Equal(t, rows[1][1], "[99,99,99]", "row 1")
		assert.Equal(t, rows[1][2], "[100,100,100]", "row 1")
		assert.Equal(t, rows[1][3], "['99','99','99']", "row 1")
		assert.Equal(t, rows[1][4], "1", "row 0")

		rows = ch.safeQuery(t, "select * from pg2ch_test.ch3 order by id desc limit 10")
		assert.Equal(t, rows[0], []string{"20000", "[100]", "[101]", "[102]", "[103]", "1"})

		repl.Finish()
	}()

	<-stopCh
}
