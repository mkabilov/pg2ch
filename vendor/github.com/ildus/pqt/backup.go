package pqt

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

type ReplicaNode struct {
	PostgresNode
	Master *PostgresNode
}

// Creates a replica for specified master node.
func MakeReplicaNode(name string, master *PostgresNode) *ReplicaNode {
	node := &ReplicaNode{*MakePostgresNode(name), master}
	return node
}

// Write default recovery.conf.
func (node *ReplicaNode) writeRecoveryConf() {
	lines := `
primary_conninfo = 'application_name=%s port=%d user=%s hostaddr=127.0.0.1'
standby_mode = on
`

	lines = fmt.Sprintf(lines, node.name, node.Port, node.user)
	confFile := filepath.Join(node.dataDirectory, "recovery.conf")
	err := ioutil.WriteFile(confFile, []byte(lines), os.ModePerm)

	if err != nil {
		log.Panic("can't write recovery configuration: ", err)
	}
}

// Initializes a replica: makes backup and writes recovery.conf.
func (node *ReplicaNode) Init(params ...string) (string, error) {
	var err error

	if node.Master.status != STARTED {
		log.Panic("master node should be started")
	}

	node.baseDirectory, err = ioutil.TempDir("", "pqt_backup_")
	if err != nil {
		log.Panic("cannot create backup base directory")
	}
	node.dataDirectory = filepath.Join(node.baseDirectory, "data")
	os.Mkdir(node.dataDirectory, 0700)

	args := []string{
		"-p", strconv.Itoa(node.Master.Port),
		"-h", node.host,
		"-U", node.user,
		"-D", node.dataDirectory,
		"-X", "fetch",
	}
	args = append(args, params...)
	res := execUtility("pg_basebackup", args...)

	node.initDefaultConf()
	node.writeRecoveryConf()
	node.status = STOPPED
	return res, nil
}

// Waits until replica gets to current LSN on master.
func (node *ReplicaNode) Catchup() {
	var lsn string

	poll_lsn := "select pg_current_wal_lsn()::text"
	wait_lsn := "select pg_last_wal_replay_lsn() >= '%s'::pg_lsn"

	rows := node.Master.Fetch("postgres", poll_lsn)
	rows.Next()

	err := rows.Scan(&lsn)
	if err != nil {
		log.Panic("failed to poll current lsn from master")
	}
	rows.Close()

	wait_query := fmt.Sprintf(wait_lsn, lsn)
	for {
		var reached bool

		rows = node.Fetch("postgres", wait_query)
		rows.Next()
		err = rows.Scan(&reached)
		rows.Close()
		if err != nil {
			log.Panic("failed to get replay lsn from replica")
		}

		if reached {
			break
		}
	}
}
