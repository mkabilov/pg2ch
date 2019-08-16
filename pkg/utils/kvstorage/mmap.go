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
	fd       int
	data     []byte
	size     int
	indexMap map[string]int
}

var (
	maxKeySize  = 248 /* 1 byte for size, and other part for text */
	dataSize    = 8
	blockSize   = maxKeySize + dataSize /* 256 */
	minFileSize = 1024 * 5 * blockSize  /* about 5 thousands keys */
)

func newMmapStorage(location string) (KVStorage, error) {
	var (
		fileSize      = minFileSize
		isNew    bool = false
	)

	flags := syscall.O_RDWR | syscall.O_CLOEXEC
	if f, err := os.Stat(location); os.IsNotExist(err) {
		flags |= syscall.O_CREAT
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

	fd, err := syscall.Open(location, flags, syscall.S_IRWXU)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %v", err)
	}

	retCode := C.flock(C.int(fd), C.LOCK_EX)
	if retCode < 0 {
		return nil, fmt.Errorf("could not lock database file: %s", location)
	}

	if isNew {
		if err := syscall.Ftruncate(fd, int64(fileSize)); err != nil {
			return nil, fmt.Errorf("truncate on %s failed: %v", location, err)
		}
	}

	var data []byte
	indexMap := make(map[string]int)
	data, err = syscall.Mmap(fd, 0, fileSize, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
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
		fd:       fd,
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

	/* TODO: extend file */
	return 0, errors.New("could not find free position in storage")
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

func msync(b []byte, blocklen int) (err error) {
	var _p0 unsafe.Pointer
	_p0 = unsafe.Pointer(&b[0])

	_, _, e1 := syscall.Syscall(syscall.SYS_MSYNC, uintptr(_p0), uintptr(blocklen), unix.MS_SYNC)
	if e1 != 0 {
		err = errnoErr(e1)
	}
	return
}
