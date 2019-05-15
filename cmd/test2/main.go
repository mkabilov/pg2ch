package main

import (
	"fmt"
	"log"
	"os"

	"github.com/peterbourgon/diskv"
)

func main() {
	d := diskv.New(diskv.Options{
		BasePath:     "diskv",
		CacheSizeMax: 1024 * 1024, // 1MB
	})

	key := "alpha"
	if err := d.Write(key, []byte{'1', '2', '3'}); err != nil {
		panic(err)
	}

	d.

	value, err := d.Read("beta")
	if err == os.ErrNotExist {
		log.Println("does not exist")
		os.Exit(0)
	}

	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", value)

	if err := d.Erase(key); err != nil {
		panic(err)
	}
}
