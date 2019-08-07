package tests

import (
	"fmt"
	"github.com/ildus/pqt"
	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/replicator"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/chload"

	"log"
	"strconv"
	"testing"
	"time"
)

const (
	initpg = `
create role postgres superuser;
create table pg1(id bigserial, a int);
alter table pg1 replica identity full;
insert into pg1(a) select i from generate_series(1, 10000) i;
`
	testConfigFile = "./test.yaml"
)

type CHConn struct {
	connString string
	chloader   *chload.CHLoad
}

var (
	ch     CHConn
	initch = []string{
		"drop database if exists pg2ch_test;",
		"create database pg2ch_test;",
		`create table pg2ch_test.ch1(id UInt64, a int, sign Int8)
			engine=CollapsingMergeTree(sign) order by id;`,
		`create table pg2ch_test.ch1_aux(id UInt64, a int, sign Int8)
			engine=CollapsingMergeTree(sign) order by id;`,
	}
)

func (ch *CHConn) safeExec(t *testing.T, sql string) {
	err := ch.chloader.Exec(sql)
	if err != nil {
		t.Fatal("could not make query:", err)
	}
}

func (ch *CHConn) safeQuery(t *testing.T, sql string) [][]string {
	rows, err := ch.chloader.Query(sql)
	if err != nil {
		t.Fatal("could not make query:", err)
	}
	return rows
}

func TestBasicSync(t *testing.T) {
	node := pqt.MakePostgresNode("master")
	defer node.Stop()

	config.DefaultPostgresPort = uint16(node.Port)
	cfg, err := config.New(testConfigFile)
	ch.chloader = chload.New(cfg.ClickHouse.ConnectionString())

	if err != nil {
		log.Fatal("config parsing error: ", err)
	}

	node.Init()
	node.AppendConf("postgresql.conf", `
log_min_messages = ERROR
hot_standby = on
wal_keep_segments = 10
wal_level = logical
max_logical_replication_workers = 10
`)
	node.AppendConf("pg_hba.conf", `
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

	go func() {
		repl := replicator.New(*cfg)
		err := repl.Run()
		if err != nil {
			t.Fatal("could not start replicator: ", err)
		}
	}()

	/* while until sync of table */
	go func() {
		counter := 0
		for {
			rows := ch.safeQuery(t, "select count(*) from ch1")
			recCount, err := strconv.Atoi(rows[0][0])
			if err != nil {
				t.Fatal(err)
			}

			if recCount == 10000-1 {
				break
			}

			counter += 1
			time.Sleep(time.Second)

			if counter >= 60 && recCount == 0 {
				t.Fatal("could not sync any data")
			}
		}
	}()
}
