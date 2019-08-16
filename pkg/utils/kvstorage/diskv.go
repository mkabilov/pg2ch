package kvstorage

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/peterbourgon/diskv"
)

type diskvStorage struct {
	location string
	storage  *diskv.Diskv
}

func newDiskvStorage(location string) (KVStorage, error) {
	storage := diskv.New(diskv.Options{
		BasePath:     location,
		CacheSizeMax: 100 * 1024 * 1024, // 100MB
	})
	return &diskvStorage{storage: storage}, nil
}

func init() {
	Register("diskv", newDiskvStorage)
}

func (s *diskvStorage) Has(key string) bool {
	return s.storage.Has(key)
}

func (s *diskvStorage) ReadLSN(key string) (dbtypes.LSN, error) {
	var lsn dbtypes.LSN

	val, err := s.storage.Read(key)
	if err != nil {
		return dbtypes.InvalidLSN, err
	}
	err = lsn.Parse(string(val))
	if err != nil {
		panic(fmt.Sprintf("corrupted lsn in storage: %s, %v", lsn, err))
	}
	return lsn, nil
}

func (s *diskvStorage) WriteLSN(key string, lsn dbtypes.LSN) error {
	return s.storage.WriteStream(key, bytes.NewReader(lsn.FormattedBytes()), true)
}

func (s *diskvStorage) ReadUint(key string) (uint64, error) {
	val, err := strconv.ParseUint(s.storage.ReadString(key), 10, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (s *diskvStorage) WriteUint(key string, val uint64) error {
	return s.storage.WriteString(key, fmt.Sprintf("%v", val))
}

func (d *diskvStorage) Keys() []string {
	var result []string
	for key := range d.storage.Keys(nil) {
		result = append(result, key)
	}
	return result
}

func (d *diskvStorage) Erase(key string) error {
	return d.storage.Erase(key)
}

func (d *diskvStorage) Close() error {
	d.storage = nil
	return nil
}
