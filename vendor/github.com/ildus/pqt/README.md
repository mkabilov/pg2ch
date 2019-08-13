pqt
=========================

`pqt` makes easier to manage postgres nodes for testing, benchmarking
and other purposes.

Now it can:

* Create, initialize, start, stop nodes with
default or custom configuration.
* Trace the nodes (with debug breakpoints).
* Make queries to the nodes.
* Get and manipulate with list of started processes.

Installation
-------------

```
go get github.com/ildus/pqt
```

Usage
------

Create a node and its replica.

```
import "github.com/ildus/pqt"
...
node := pqt.MakePostgresNode("master")
node_replica := pqt.MakeReplicaNode("replica", node)
```

Initialize the nodes.

```
node.Init()
node_replica.Init()
```

Change the configuration.

```
node.AppendConf("postgresql.conf", "log_statement=all")
```

Start the nodes and make replica to catchup to master.

```
node.Init()
node.Start()

node_replica.Init()
node_replica.Start()
node_replica.Catchup()
```

Get some data from the node.

```
var pid int
rows := node.Fetch("postgres", "select pg_backend_pid()")
for rows.Next() {
	rows.Scan(&pid)
	break
}
rows.Close()
```

Or make a query without returned data.

```
node.Execute("postgres", "create table one(a text)")
```

Or make a connection and reuse it for queries.

```
conn := pqt.MakePostgresConn(node, "postgres")
conn.Execute("discard all");
```

Get node processes and put some breakpoint on some of them.
This is mostly useful for extension testing.

```
process := node.GetProcess()
children := process.Children()
for _, child := range children {
	if child.Pid == 4567 {
		debugger = pqt.MakeDebugger(child)
		breakpoint = debugger.CreateBreakpoint("pg_backend_pid", func() error {
			catched += 1
			return nil
		})
	}
}
```
