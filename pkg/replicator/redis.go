package replicator

import (
	"fmt"
	"log"
	"strings"

	"github.com/tidwall/redcon"
)

const forbiddenError = "cannot modify '" + tableLSNKeyPrefix + "*' keys"

func (r *Replicator) caskRedis() {
	err := redcon.ListenAndServe(r.cfg.RedisBind,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				if err := conn.Close(); err != nil {
					log.Printf("could not close redis connection: %v", err)
				}
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				key := string(cmd.Args[1])
				value := cmd.Args[2]

				if strings.HasPrefix(key, tableLSNKeyPrefix) {
					conn.WriteString(fmt.Sprintf("ERR: %s", forbiddenError))
					return
				}

				if err := r.caskDB.Put(key, value); err != nil {
					conn.WriteString(fmt.Sprintf("ERR: %s", err))
				} else {
					conn.WriteString("OK")
				}
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				key := string(cmd.Args[1])
				value, err := r.caskDB.Get(key)
				if err != nil {
					conn.WriteNull()
				} else {
					conn.WriteBulk(value)
				}
			case "keys":
				conn.WriteArray(r.caskDB.Len())
				for key := range r.caskDB.Keys() {
					conn.WriteBulk([]byte(key))
				}
			case "exists":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				key := string(cmd.Args[1])
				if r.caskDB.Has(key) {
					conn.WriteInt(1)
				} else {
					conn.WriteInt(0)
				}
			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				key := string(cmd.Args[1])

				err := r.caskDB.Delete(key)
				if err != nil {
					conn.WriteInt(0)
				} else {
					conn.WriteInt(1)
				}
			case "pause":
				//TODO
				conn.WriteString("OK")
			case "resume":
				//TODO
				conn.WriteString("OK")
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			}
		},

		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) {},
	)

	if err != nil {
		select {
		case r.errCh <- err:
		default:
		}
	}
}
