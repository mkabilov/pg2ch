package kvstorage

import (
	"fmt"

	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

type KVStorage interface {
	Has(key string) bool
	ReadLSN(key string) (dbtypes.LSN, error)
	WriteLSN(key string, lsn dbtypes.LSN) error
	ReadUint(key string) (uint64, error)
	WriteUint(key string, val uint64) error
	Keys() []string
	Erase(key string) error
}
type newStorageFunc = func(string) (KVStorage, error)

var (
	storages = make(map[string]newStorageFunc)
)

func New(stype string, location string) (storage KVStorage, err error) {
	if f, ok := storages[stype]; ok {
		storage, err = f(location)
		return
	}

	panic(fmt.Sprintf("could not find %s type storage", stype))
}

func Register(stype string, f newStorageFunc) {
	storages[stype] = f
}
