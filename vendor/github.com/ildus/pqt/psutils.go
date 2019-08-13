package pqt

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"strconv"
	"strings"
)

type ProcessType byte

const (
	UnknownProcess             ProcessType = iota
	Postmaster                 ProcessType = iota
	AutovacuumLauncher         ProcessType = iota
	AutovacuumWorker           ProcessType = iota
	BackgroundWriter           ProcessType = iota
	Checkpointer               ProcessType = iota
	LogicalReplicationLauncher ProcessType = iota
	Startup                    ProcessType = iota
	StatsCollector             ProcessType = iota
	WalReceiver                ProcessType = iota
	WalSender                  ProcessType = iota
	WalWriter                  ProcessType = iota
	OtherBgWorker              ProcessType = iota
)

type Process struct {
	Type      ProcessType
	CmdLine   string
	Pid       int
	ParentPid int
}

func (process *Process) Children() (result []*Process) {
	var out bytes.Buffer
	cmd := exec.Command("pgrep", "-P", strconv.Itoa(process.Pid))
	cmd.Stdout = &out
	err := cmd.Run()

	if err != nil {
		log.Panic("pgrep launch error: ", err)
	}

	pids := strings.Split(out.String(), "\n")
	for _, spid := range pids {
		if spid == "" {
			continue
		}

		pid, err := strconv.Atoi(spid)
		if err != nil {
			log.Panicf("can't convert pgrep line ('%s') to int", spid)
		}
		child := getProcessByPid(pid)
		if child != nil {
			result = append(result, child)
		}
	}
	return result
}

func getProcessType(process *Process) (result ProcessType) {
	ProcessTypeBasicIdent := map[ProcessType][]string{
		AutovacuumLauncher: []string{"autovacuumlauncher"},
		AutovacuumWorker:   []string{"autovacuumworker"},
		BackgroundWriter:   []string{"backgroundwriter", "writer"},
		Checkpointer:       []string{"checkpointer"},
		Startup:            []string{"startup"},
		StatsCollector:     []string{"statscollector"},
		WalReceiver:        []string{"walreceiver"},
		WalSender:          []string{"walsender"},
		WalWriter:          []string{"walwriter"},
		LogicalReplicationLauncher: []string{"logicalreplicationlauncher",
			"logicalreplicationworker"},
	}

	result = UnknownProcess

	cmdline := process.CmdLine
	cmdline = strings.Replace(cmdline, " ", "", -1)
	cmdline = strings.Replace(cmdline, "postgres:", "", 1)

	tmp := cmdline
	cmdline = strings.Replace(cmdline, "bgworker:", "", 1)
	is_bgworker := tmp != cmdline

	for ptype, items := range ProcessTypeBasicIdent {
		for _, item := range items {
			if strings.HasPrefix(cmdline, item) {
				result = ptype
				goto end
			}
		}
	}

	if result == UnknownProcess && is_bgworker {
		result = OtherBgWorker
	}

end:
	return result
}

func getProcessByPid(pid int) (result *Process) {
	if pid <= 0 {
		return nil
	}

	procDir := fmt.Sprintf("/proc/%d/", pid)
	cmdline, err := ioutil.ReadFile(procDir + "cmdline")
	if err != nil {
		log.Panicf("can't read /proc/%d/cmdline", pid)
	}

	statline, err := ioutil.ReadFile(procDir + "stat")
	if err != nil {
		log.Panicf("can't read /proc/%d/stat", pid)
	}
	ppid, err := strconv.Atoi(strings.Split(string(statline), " ")[3])
	if err != nil {
		log.Panicf("can't read parent pid")
	}

	result = &Process{
		Pid:       pid,
		CmdLine:   string(cmdline),
		ParentPid: ppid,
	}
	result.Type = getProcessType(result)
	return result
}
