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

	assert := assert.New(t)

	storage.WriteUint("key1", 1000)
	storage.WriteUint("key2", 2000)
	storage.WriteUint("key1", 1000000)

	val1, err := storage.ReadUint("key1")
	assert.Nil(err)
	val2, err := storage.ReadUint("key2")
	assert.Nil(err)

	assert.Equal(uint64(1000000), val1, "key1")
	assert.Equal(uint64(2000), val2, "key2")
	assert.True(storage.Has("key1"))
	assert.True(storage.Has("key2"))
	assert.False(storage.Has("key3"))

	storage.Erase("key1")
	assert.False(storage.Has("key1"))
	storage.WriteUint("key3", 4000000)

	var keys []string
	for key := range storage.Keys(nil) {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	assert.Equal("key2", keys[0])
	assert.Equal("key3", keys[1])

	var (
		lsn  dbtypes.LSN
		lsn2 dbtypes.LSN
	)

	lsn.Parse("0/1F0DF88")
	err = storage.WriteLSN("lsn1", lsn)
	assert.Nil(err)

	lsn2, err = storage.ReadLSN("lsn1")
	assert.Equal(lsn, lsn2, "lsn check")
	assert.Equal("0/1F0DF88", lsn2.String(), "lsn string check")
}

func TestDiskv(t *testing.T) {
	db_path, err := ioutil.TempDir("", "pg2ch_diskv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db_path)

	testStorage(t, kvstorage.New("diskv", db_path))
}
