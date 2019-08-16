package kvstorage_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/mkabilov/pg2ch/pkg/utils/kvstorage"
)

func TestMmap(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "pg2ch_mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	var storage kvstorage.KVStorage
	storage, err = kvstorage.New("mmap", tmpfile.Name())
	if err != nil {
		testStorage(t, storage)
	}
}
