package tests

import (
	"fmt"
	"github.com/ildus/pqt"
	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/replicator"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"

	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	initpg = `
create role postgres superuser login;
create table pg1(id bigserial, a int);
alter table pg1 replica identity full;
insert into pg1(a) select i from generate_series(1, 10000) i;
`
	addsql = `
insert into pg1(a) select i from generate_series(1, 100) i;
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
			a int,
			sign Int8
		 ) engine=CollapsingMergeTree(sign) order by id;`,
		`create table pg2ch_test.ch1_aux(
			id UInt64,
			a int,
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
	db_path, err := ioutil.TempDir("", "pg2ch_test_dat")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(db_path)

	node := pqt.MakePostgresNode("master")
	defer node.Stop()

	config.DefaultPostgresPort = uint16(node.Port)
	cfg, err := config.New(testConfigFile)
	cfg.PersStoragePath = db_path
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
	go func() {
		repl = replicator.New(cfg)
		err := repl.Run()
		if err != nil {
			t.Fatal("could not start replicator: ", err)
		}
	}()

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

	repl.Finish()
}
