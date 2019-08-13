package pqt

import (
	"bytes"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	currentPort int = 9999
)

func execUtility(name string, args ...string) string {
	var out bytes.Buffer
	var errout bytes.Buffer

	cmd := exec.Command(getBinPath(name), args...)
	cmd.Stdout = &out
	cmd.Stderr = &errout
	err := cmd.Run()

	if err != nil {
		log.Print(errout.String())
		log.Panic(name, " launch error: ", err)
	}

	return out.String()
}

func getBinPath(filename string) string {
	if path, _ := filepath.Abs(filename); path == filename {
		return filename
	}

	bindir := getPgConfig()["BINDIR"]
	return filepath.Join(bindir, filename)
}

func getPgConfig() map[string]string {
	var pg_config_path string
	var out bytes.Buffer

	result := make(map[string]string)

	if len(os.Getenv("PG_CONFIG")) > 0 {
		pg_config_path = os.Getenv("PG_CONFIG")
	} else {
		path, err := exec.LookPath("pg_config")
		if err != nil {
			log.Panic("pg_config is not found in $PATH")
		}
		pg_config_path = path
	}

	if _, err := os.Stat(pg_config_path); os.IsNotExist(err) {
		log.Panic("pg_config is not found")
	}

	cmd := exec.Command(pg_config_path)
	cmd.Stdout = &out
	err := cmd.Run()

	if err != nil {
		log.Panic("pg_config launch error: ", err)
	}

	lines := strings.Split(out.String(), "\n")
	for i := range lines {
		line := strings.Split(lines[i], " = ")
		if len(line) > 1 {
			result[line[0]] = line[1]
		} else {
			result[line[0]] = ""
		}
	}
	return result
}

func getAvailablePort() int {
	var initial int = currentPort + 1

	for {
		currentPort += 1
		conn, _ := net.DialTimeout("tcp",
			net.JoinHostPort("127.0.0.1", strconv.Itoa(currentPort)), 10*time.Second)

		if conn == nil {
			break
		}

		conn.Close()
		if currentPort-initial > 10 {
			log.Panicf("can't allocate a port (tested %d - %d)",
				initial, currentPort-1)
		}
	}

	return currentPort
}
