// +build linux freebsd openbsd netbsd dragonfly darwin

package kvstorage_test

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mkabilov/pg2ch/pkg/utils/kvstorage"
)

func TestMmapBasic(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "pg2ch_mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	var storage kvstorage.KVStorage
	storage, err = kvstorage.New("mmap", tmpfile.Name())
	assert.Nil(t, err)
	testStorage(t, storage)
}

func TestMmapExtend(t *testing.T) {
	assert := assert.New(t)

	tmpfile, err := ioutil.TempFile("", "pg2ch_mmap_extend")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	var storage kvstorage.KVStorage
	storage, err = kvstorage.New("mmap", tmpfile.Name())
	assert.Nil(err)

	f, _ := os.Stat(tmpfile.Name())
	size := f.Size()

	for i := 0; i < kvstorage.MinKeysCount; i++ {
		key := "key" + strconv.Itoa(i)
		err := storage.WriteUint(key, uint64(i))
		assert.Nil(err)
	}

	/* check values */
	for i := 0; i < kvstorage.MinKeysCount; i++ {
		key := "key" + strconv.Itoa(i)
		val, err := storage.ReadUint(key)
		assert.Nil(err)
		assert.Equal(uint64(i), val)
	}

	f, _ = os.Stat(tmpfile.Name())
	size2 := f.Size()
	assert.Equal(size, size2, "size at beginning")

	for i := kvstorage.MinKeysCount; i < kvstorage.MinKeysCount*2; i++ {
		key := "key" + strconv.Itoa(i)
		err := storage.WriteUint(key, uint64(i))
		assert.Nil(err)
	}

	/* check values */
	for i := 0; i < kvstorage.MinKeysCount*2; i++ {
		key := "key" + strconv.Itoa(i)
		val, err := storage.ReadUint(key)
		assert.Nil(err)
		assert.Equal(uint64(i), val)
	}

	f, _ = os.Stat(tmpfile.Name())
	size3 := f.Size()
	assert.Equal(size*2, size3, "size at end should twice bigger")
	err = storage.Close()
	assert.Nil(err)

	/* reopen and check all keys */
	storage, err = kvstorage.New("mmap", tmpfile.Name())
	assert.Nil(err)

	for i := 0; i < kvstorage.MinKeysCount*2; i++ {
		key := "key" + strconv.Itoa(i)
		val, err := storage.ReadUint(key)
		assert.Nil(err)
		assert.Equal(uint64(i), val)
	}

	f, _ = os.Stat(tmpfile.Name())
	size = f.Size()
	assert.Equal(size, size3, "size should equal old size")
}
