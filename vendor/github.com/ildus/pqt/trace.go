// +build linux

package pqt

import (
	"debug/dwarf"
	"debug/elf"
	"fmt"
	sys "golang.org/x/sys/unix"
	"io"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"syscall"
)

type BreakpointCallback func() error

type Breakpoint struct {
	addr        uint64
	pcaddr      uint64
	original    []byte
	callback    BreakpointCallback
	Description string
}

type DebugInformation struct {
	dwarfData *dwarf.Data
}

type Debugger struct {
	Process        *Process
	BreakpointChan chan *Breakpoint
	Thread         Pthread
	Breakpoints    map[uint64]*Breakpoint
	DebugInfo      *DebugInformation
}

func (di *DebugInformation) getFirstLineAddr(cu *dwarf.Entry, addr uint64) uint64 {
	lineReader, err := di.dwarfData.LineReader(cu)
	if err != nil {
		log.Fatal("can't read debug_line: ", err)
	}
	if lineReader == nil {
		log.Fatal("there is no debug_line in executable")
	}

	var entry dwarf.LineEntry

	next := false
	for {
		err := lineReader.Next(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic("dwarf line reading error: ", err)
		}
		if entry.Address == addr {
			next = true
			continue
		}

		if next {
			return entry.Address
		}
	}
	return 0
}

func (di *DebugInformation) LookupFunction(funcName string) (uint64, error) {
	var cu *dwarf.Entry = nil
	reader := di.dwarfData.Reader()

	for {
		entry, err := reader.Next()
		if err != nil {
			log.Panic("dwarf data reading error: ", err)
		}
		if entry == nil {
			break
		}

		if entry.Tag == dwarf.TagCompileUnit {
			cu = entry
		} else if entry.Tag == dwarf.TagSubprogram {
			if cu == nil {
				log.Fatal("complilation unit not found for function")
			}

			name := entry.Val(dwarf.AttrName).(string)
			if name != funcName {
				continue
			}

			addrAttr := entry.Val(dwarf.AttrLowpc)
			if addrAttr == nil {
				return 0, fmt.Errorf("symbol %q has no LowPC attribute", name)
			}
			addr, ok := addrAttr.(uint64)
			if !ok {
				return 0, fmt.Errorf("symbol %q has non-uint64 LowPC attribute", name)
			}
			return di.getFirstLineAddr(cu, addr), nil
		}
	}
	return 0, fmt.Errorf("function is not found")
}

func getDebugInformation(path string) *DebugInformation {
	f, err := elf.Open(path)
	if err != nil {
		log.Panic("can't open binary: ", err)
	}

	data, err := f.DWARF()
	if err != nil {
		log.Panic("can't get dwarf information from binary: ", err)
	}
	di := &DebugInformation{
		dwarfData: data,
	}
	return di
}

func getFirstInstructionAddress(pid int) uint64 {
	dat, err := ioutil.ReadFile(fmt.Sprintf("/proc/%d/maps", pid))
	res, err := strconv.ParseUint(strings.Split(string(dat), "-")[0], 16, 64)
	if err != nil {
		log.Println("can't parse first instruction address")
	}
	return res
}

func setPC(pid int, pc uint64) {
	var regs syscall.PtraceRegs
	err := syscall.PtraceGetRegs(pid, &regs)
	if err != nil {
		log.Fatal(err)
	}
	regs.SetPC(pc)
	err = syscall.PtraceSetRegs(pid, &regs)
	if err != nil {
		log.Fatal(err)
	}
}

func getPC(pid int) uint64 {
	var regs syscall.PtraceRegs
	err := syscall.PtraceGetRegs(pid, &regs)
	if err != nil {
		log.Fatal(err)
	}
	return regs.PC()
}

func writeBreakpoint(pid int, breakpoint uintptr) []byte {
	original := make([]byte, 1)
	_, err := syscall.PtracePeekData(pid, breakpoint, original)
	if err != nil {
		log.Fatal("can't peek data for breakpoint: ", err)
	}
	_, err = syscall.PtracePokeData(pid, breakpoint, []byte{0xCC})
	if err != nil {
		log.Fatal("can't poke data for breakpoint: ", err)
	}
	return original
}

func clearBreakpoint(pid int, breakpoint uintptr, original []byte) {
	_, err := syscall.PtracePokeData(pid, breakpoint, original)
	if err != nil {
		log.Fatal("can't poke data that removes breakpoint: ", err)
	}
}

func MakeDebugger(p *Process) *Debugger {
	debugger := &Debugger{
		Process:        p,
		BreakpointChan: make(chan *Breakpoint, 1),
		DebugInfo:      getDebugInformation(getBinPath("postgres")),
		Breakpoints:    make(map[uint64]*Breakpoint),
	}

	thread := makeThread(func() {
		var ws syscall.WaitStatus

		pgid, err := syscall.Getpgid(p.Pid)
		if err != nil {
			log.Fatal("can't get pgid: ", err)
		}

		err = syscall.PtraceAttach(p.Pid)
		if err != nil {
			log.Fatal("can't attach: ", err)
		}
		// should stop after attach
		_, err = syscall.Wait4(p.Pid, &ws, syscall.WALL, nil)
		if !ws.Stopped() {
			log.Fatal("could not attach: ", err)
		}
		startingPC := getFirstInstructionAddress(p.Pid)
		syscall.PtraceCont(p.Pid, 0)

		for {
			wpid, err := syscall.Wait4(-1*pgid, &ws, syscall.WALL, nil)
			if err != nil {
				log.Fatal("wait4 error ", err)
			}
			if wpid == 0 {
				continue
			}

			if ws.Signaled() || ws.Exited() {
				log.Println("tracee process has exited")
				break
			} else if ws.Stopped() {
				curAddr := getPC(p.Pid)
				if ws.StopSignal() == sys.SIGTRAP {
					addr := curAddr - 1
					br, ok := debugger.Breakpoints[addr]
					if !ok {
						log.Printf("trap on at %x, no breakpoint", addr)
					} else {
						log.Printf("trap on '%s' at %x", br.Description, addr)

						/* remove trap instruction so it can run safely */
						clearBreakpoint(p.Pid, uintptr(addr), br.original)
						br.callback()
						setPC(p.Pid, addr)

						/* make single step and restore breakpoint */
						syscall.PtraceSingleStep(p.Pid)
						_, err := syscall.Wait4(p.Pid, &ws, syscall.WALL, nil)
						if err != nil {
							log.Fatal("single step wait4 error ", err)
						}
						writeBreakpoint(p.Pid, uintptr(addr))
					}
				} else {
					select {
					case br := <-debugger.BreakpointChan:
						if br.pcaddr != 0 {
							log.Printf("remove a breakpoint on '%s' at %x",
								br.Description, br.pcaddr)
							clearBreakpoint(p.Pid, uintptr(br.pcaddr), br.original)
							delete(debugger.Breakpoints, br.pcaddr)
						} else {
							/* we got a new breakpoint */
							resaddr := startingPC + br.addr
							log.Printf("putting a breakpoint on '%s' at %x",
								br.Description, resaddr)
							br.original = writeBreakpoint(p.Pid, uintptr(resaddr))
							br.pcaddr = resaddr
							debugger.Breakpoints[resaddr] = br
						}
					default:
						log.Printf("tracee has stopped with reason '%s' on %x", ws.StopSignal(),
							curAddr)
						goto outside
					}
				}
			}
			syscall.PtraceCont(p.Pid, 0)
		}

	outside:
		syscall.PtraceDetach(p.Pid)
		log.Println("debugger thread has ended")
	})
	debugger.Thread = thread
	return debugger
}

func (debugger *Debugger) CreateBreakpoint(funcName string,
	callback BreakpointCallback) *Breakpoint {

	addr, err := debugger.DebugInfo.LookupFunction(funcName)
	if addr == 0 || err != nil {
		log.Fatal("can't find function addr: ", err)
	}
	br := &Breakpoint{
		addr:        addr,
		callback:    callback,
		Description: funcName,
	}
	debugger.BreakpointChan <- br
	syscall.Kill(debugger.Process.Pid, syscall.SIGSTOP)
	return br
}

func (debugger *Debugger) RemoveBreakpoint(br *Breakpoint) {
	debugger.BreakpointChan <- br
	syscall.Kill(debugger.Process.Pid, syscall.SIGSTOP)
}

func (debugger *Debugger) Detach() {
	err := syscall.PtraceDetach(debugger.Process.Pid)
	if err != nil {
		log.Fatal("can't detach: ", err)
	}
}

func (debugger *Debugger) Stop() {
	debugger.Thread.Kill()
}

func (debugger *Debugger) SigStop() error {
	return sys.Kill(debugger.Process.Pid, sys.SIGSTOP)
}

func (debugger *Debugger) SigContinue() error {
	return sys.Kill(debugger.Process.Pid, sys.SIGCONT)
}
