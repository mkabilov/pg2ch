package internal

import (
	"hash/crc32"

	pb "github.com/prologic/bitcask/internal/proto"
)

func NewEntry(key string, value []byte) pb.Entry {
	checksum := crc32.ChecksumIEEE(value)

	return pb.Entry{
		Checksum: checksum,
		Key:      key,
		Value:    value,
	}
}
