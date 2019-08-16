// +build linux freebsd openbsd netbsd dragonfly darwin

package kvstorage

// #include <sys/file.h>
import "C"
import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"golang.org/x/sys/unix"
)

type mmapStorage struct {
	lock     sync.RWMutex
	location string
	data     []byte
	size     int
	indexMap map[string]int
}

var (
	MinKeysCount = 100
	blockSize    = 256
	dataSize     = 8 // uint64
	maxKeySize   = blockSize - dataSize
	minFileSize  = MinKeysCount * blockSize
)

func openStorage(location string, fileSize int, flags int, truncate bool) (*os.File, error) {
	file, err := os.OpenFile(location, flags, 0644)
	if err != nil {
		return nil, fmt.Errorf("could not open storage file: %v", err)
	}

	retCode := C.flock(C.int(file.Fd()), C.LOCK_EX)
	if retCode < 0 {
		return nil, fmt.Errorf("could not lock database file: %s", location)
	}

	if truncate {
		if err := file.Truncate(int64(fileSize)); err != nil {
			return nil, fmt.Errorf("truncate on %s failed: %v", location, err)
		}
	}

	retCode = C.flock(C.int(file.Fd()), C.LOCK_UN)
	if retCode < 0 {
		return nil, fmt.Errorf("could not unlock database file: %s", location)
	}

	return file, nil
}

func mapFile(file *os.File, fileSize int) ([]byte, error) {
	var data, err = syscall.Mmap(int(file.Fd()), 0, fileSize,
		syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)

	if err != nil {
		return nil, fmt.Errorf("could not map storage file: %v", err)
	}
	return data, nil
}

func newMmapStorage(location string) (KVStorage, error) {
	var (
		file     *os.File
		fileSize      = minFileSize
		isNew    bool = false
	)

	flags := os.O_RDWR | unix.O_CLOEXEC
	if f, err := os.Stat(location); os.IsNotExist(err) {
		flags |= os.O_CREATE
		isNew = true
	} else if err != nil {
		return nil, fmt.Errorf("could not stat file %s: %v", location, err)
	} else {
		if sz := f.Size(); sz == 0 {
			isNew = true
		} else {
			fileSize = int(sz)
		}
	}

	file, err := openStorage(location, fileSize, flags, isNew)
	if err != nil {
		return nil, err
	}
	// we can safely close after mapping
	defer file.Close()

	data, err := mapFile(file, fileSize)
	if err != nil {
		return nil, err
	}

	indexMap := make(map[string]int)
	if !isNew {
		for pos := 0; pos < fileSize; pos += blockSize {
			if data[pos] == 0x00 {
				/* end of keys */
				break
			}

			keysize := int(data[pos])
			indexMap[string(data[pos+1:pos+1+keysize])] = pos
		}
	} else {
		for pos := 0; pos < fileSize; pos += 1 {
			data[pos] = 0x00
		}
		unix.Msync(data, unix.MS_SYNC)
	}
	return &mmapStorage{
		location: location,
		data:     data,
		size:     fileSize,
		indexMap: indexMap,
	}, nil
}

func (s *mmapStorage) Has(key string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	_, ok := s.indexMap[key]
	return ok
}

func (s *mmapStorage) Erase(key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	pos, ok := s.indexMap[key]
	if !ok {
		return errors.New("key not exists in storage")
	}

	for i := 0; i < blockSize; i++ {
		s.data[pos+i] = 0x00
	}
	msync(s.data[pos:], blockSize)
	delete(s.indexMap, key)
	return nil
}

func (s *mmapStorage) findFreePos() (int, error) {
	for pos := 0; pos < s.size; pos += blockSize {
		if s.data[pos] == 0x00 {
			return pos, nil
		}
	}

	err := syscall.Munmap(s.data)
	if err != nil {
		return 0, fmt.Errorf("could not unmap storage file: %s", err)
	}

	oldSize := s.size
	s.size += minFileSize /* new portion of keys */
	file, err := openStorage(s.location, s.size, syscall.O_RDWR, true)
	if err != nil {
		return 0, fmt.Errorf("could not extend file: %v", err)
	}
	defer file.Close()

	s.data, err = mapFile(file, s.size)
	if err != nil {
		return 0, fmt.Errorf("could not map extended file: %v", err)
	}

	// fill with zeroes
	for pos := oldSize; pos < s.size; pos += 1 {
		s.data[pos] = 0x00
	}
	unix.Msync(s.data, unix.MS_SYNC)
	return oldSize, nil
}

func (s *mmapStorage) WriteUint(key string, val uint64) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(key) > maxKeySize-1 {
		return fmt.Errorf("too long key for storage, limit is %d", maxKeySize-1)
	}
	pos, ok := s.indexMap[key]
	if !ok {
		var err error
		if pos, err = s.findFreePos(); err != nil {
			return err
		}
	}

	s.indexMap[key] = pos
	s.data[pos] = byte(len(key))
	copy(s.data[pos+1:], []byte(key))
	binary.BigEndian.PutUint64(s.data[pos+maxKeySize:], val)
	msync(s.data[pos:], blockSize)
	return nil
}

func (s *mmapStorage) ReadUint(key string) (uint64, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	pos, ok := s.indexMap[key]
	if !ok {
		return 0, fmt.Errorf("no such key in storage: %s", key)
	}

	return binary.BigEndian.Uint64(s.data[pos+maxKeySize:]), nil
}

func (s *mmapStorage) ReadLSN(key string) (dbtypes.LSN, error) {
	val, err := s.ReadUint(key)
	return dbtypes.LSN(val), err
}

func (s *mmapStorage) WriteLSN(key string, lsn dbtypes.LSN) error {
	return s.WriteUint(key, uint64(lsn))
}

func (s *mmapStorage) Keys() []string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var result []string
	for key := range s.indexMap {
		result = append(result, key)
	}
	return result
}

func (s *mmapStorage) Close() error {
	err := syscall.Munmap(s.data)
	if err != nil {
		return fmt.Errorf("could not unmap storage file: %s", err)
	}

	s.indexMap = nil
	s.data = nil

	return nil
}

func init() {
	Register("mmap", newMmapStorage)
}

// errnoErr returns common boxed Errno values, to prevent
// allocations at runtime.
func errnoErr(e syscall.Errno) error {
	switch e {
	case 0:
		return nil
	case unix.EAGAIN:
		return syscall.EINVAL
	case unix.ENOENT:
		return syscall.ENOENT
	}
	return e
}

func msync(b []byte, blocklen int) error {
	_p0 := unsafe.Pointer(&b[0])

	_, _, e1 := syscall.Syscall(syscall.SYS_MSYNC, uintptr(_p0), uintptr(blocklen), unix.MS_SYNC)
	if e1 != 0 {
		return errnoErr(e1)
	}
	return nil
}
