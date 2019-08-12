package replicator

import (
	"fmt"
	"strings"

	"github.com/tidwall/redcon"

	"github.com/mkabilov/pg2ch/pkg/config"
)

const forbiddenError = "cannot modify '" + config.TableLSNKeyPrefix + "*' keys"

func (r *Replicator) startRedisServer() {
	r.logger.Infof("starting redis-like server: %v", r.cfg.RedisBind)
	redisServer := redcon.NewServer(r.cfg.RedisBind,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				if err := conn.Close(); err != nil {
					r.logger.Warnf("could not close redis connection: %v", err)
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
				key := config.TableLSNKeyPrefix + string(cmd.Args[1])
				value, err := r.persStorage.Read(key)
				if err != nil {
					conn.WriteNull()
				} else {
					conn.WriteBulk(value)
				}
			case "keys":
				for key := range r.persStorage.Keys(nil) {
					if !strings.HasPrefix(key, config.TableLSNKeyPrefix) {
						continue
					}

					conn.WriteString(key[len(config.TableLSNKeyPrefix):])
				}
				conn.WriteString("OK")
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
				conn.WriteString(r.pause())
			case "resume":
				conn.WriteString(r.resume())
			case "status":
				conn.WriteString(r.curState.String())
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			}
		},

		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) {},
	)

	if err := redisServer.ListenAndServe(); err != nil {
		select {
		case r.errCh <- err:
		default:
		}
	}
}
