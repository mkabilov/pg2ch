package pqt

import (
	"database/sql"
	"log"
)

type PostgresConn struct {
	node    *PostgresNode
	dbname  string
	conn    *sql.DB
	process *Process
}

func MakePostgresConn(node *PostgresNode, dbname string) *PostgresConn {
	return &PostgresConn{
		node:    node,
		dbname:  dbname,
		conn:    node.Connect(dbname),
		process: nil,
	}
}

// Execute query and fetch resulting rows from node.
func (conn *PostgresConn) Fetch(sql string, params ...interface{}) *sql.Rows {
	rows, err := conn.conn.Query(sql, params...)
	if err != nil {
		log.Panic(err)
	}

	return rows
}

// Executes query without returning any data.
func (conn *PostgresConn) Execute(sql string, params ...interface{}) {
	conn.Fetch(sql, params...).Close()
}

// Get backend process
func (conn *PostgresConn) Process() *Process {
	if conn.process == nil {
		var pid int
		rows := conn.Fetch("select pg_backend_pid()")
		defer rows.Close()
		rows.Next()
		rows.Scan(&pid)
		conn.process = getProcessByPid(pid)
	}

	return conn.process
}

// Close the connection
func (conn *PostgresConn) Close() {
	conn.conn.Close()
}
