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
	Keys(cancel <-chan struct{}) <-chan string
	Erase(key string) error
}
type newStorageFunc = func(string) KVStorage

var (
	storages = make(map[string]newStorageFunc)
)

func New(stype string, location string) KVStorage {
	if f, ok := storages[stype]; ok {
		return f(location)
	}

	panic(fmt.Sprintf("could not find %s type storage", stype))
}

func Register(stype string, f newStorageFunc) {
	storages[stype] = f
}
