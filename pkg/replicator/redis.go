package replicator

import (
	"fmt"
	"log"
	"strings"

	"github.com/tidwall/redcon"

	"github.com/mkabilov/pg2ch/pkg/config"
)

const forbiddenError = "cannot modify '" + config.TableLSNKeyPrefix + "*' keys"

func (r *Replicator) redisServer() {
	log.Printf("listening on %s", r.cfg.RedisBind)

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
			case "lsn":
				conn.WriteString(fmt.Sprintf("last final LSN: %v", r.consumer.CurrentLSN().String()))
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				key := string(cmd.Args[1])
				value := cmd.Args[2]

				if strings.HasPrefix(key, config.TableLSNKeyPrefix) {
					conn.WriteString(fmt.Sprintf("ERR: %s", forbiddenError))
					return
				}

				if err := r.persStorage.Write(key, value); err != nil {
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
				value, err := r.persStorage.Read(key)
				if err != nil {
					conn.WriteNull()
				} else {
					conn.WriteBulk(value)
				}
			case "keys":
				conn.WriteString("OK")
				//TODO
			case "exists":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				key := string(cmd.Args[1])
				if r.persStorage.Has(key) {
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

				err := r.persStorage.Erase(key)
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
