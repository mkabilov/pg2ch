package kvstorage_test

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/kvstorage"
	"github.com/stretchr/testify/assert"
)

func testStorage(t *testing.T, storage kvstorage.KVStorage) {
	var err error

	asrt := assert.New(t)

	storage.WriteUint("key1", 1000)
	storage.WriteUint("key2", 2000)
	storage.WriteUint("key1", 1000000)

	val1, err := storage.ReadUint("key1")
	asrt.Nil(err)
	val2, err := storage.ReadUint("key2")
	asrt.Nil(err)

	asrt.Equal(uint64(1000000), val1, "key1")
	asrt.Equal(uint64(2000), val2, "key2")
	asrt.True(storage.Has("key1"))
	asrt.True(storage.Has("key2"))
	asrt.False(storage.Has("key3"))

	storage.Erase("key1")
	asrt.False(storage.Has("key1"))
	storage.WriteUint("key3", 4000000)

	keys := storage.Keys()
	sort.Strings(keys)
	asrt.Equal("key2", keys[0])
	asrt.Equal("key3", keys[1])

	var (
		lsn  dbtypes.LSN
		lsn2 dbtypes.LSN
	)

	lsn.Parse("0/1F0DF88")
	err = storage.WriteLSN("lsn1", lsn)
	asrt.Nil(err)

	lsn2, err = storage.ReadLSN("lsn1")
	asrt.Equal(lsn, lsn2, "lsn check")
	asrt.Equal("0/1F0DF88", lsn2.String(), "lsn string check")
}

func TestDiskv(t *testing.T) {
	dbPath, err := ioutil.TempDir("", "pg2ch_diskv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dbPath)

	var storage kvstorage.KVStorage
	storage, err = kvstorage.New("diskv", dbPath)
	assert.Nil(t, err)
	testStorage(t, storage)
}
