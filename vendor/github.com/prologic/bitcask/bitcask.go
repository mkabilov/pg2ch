package bitcask

import (
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gofrs/flock"
	"github.com/prologic/trie"

	"github.com/prologic/bitcask/internal"
)

var (
	// ErrKeyNotFound is the error returned when a key is not found
	ErrKeyNotFound = errors.New("error: key not found")

	// ErrKeyTooLarge is the error returned for a key that exceeds the
	// maximum allowed key size (configured with WithMaxKeySize).
	ErrKeyTooLarge = errors.New("error: key too large")

	// ErrValueTooLarge is the error returned for a value that exceeds the
	// maximum allowed value size (configured with WithMaxValueSize).
	ErrValueTooLarge = errors.New("error: value too large")

	// ErrChecksumFailed is the error returned if a key/valie retrieved does
	// not match its CRC checksum
	ErrChecksumFailed = errors.New("error: checksum failed")

	// ErrDatabaseLocked is the error returned if the database is locked
	// (typically opened by another process)
	ErrDatabaseLocked = errors.New("error: database locked")
)

// Bitcask is a struct that represents a on-disk LSM and WAL data structure
// and in-memory hash of key/value pairs as per the Bitcask paper and seen
// in the Riak database.
type Bitcask struct {
	mu sync.RWMutex

	*flock.Flock

	config    *config
	path      string
	curr      *internal.Datafile
	keydir    *internal.Keydir
	datafiles []*internal.Datafile
	trie      *trie.Trie
}

// Close closes the database and removes the lock. It is important to call
// Close() as this is the only wat to cleanup the lock held by the open
// database.
func (b *Bitcask) Close() error {
	defer func() {
		b.Flock.Unlock()
		os.Remove(b.Flock.Path())
	}()

	for _, df := range b.datafiles {
		df.Close()
	}
	return b.curr.Close()
}

// Sync flushes all buffers to disk ensuring all data is written
func (b *Bitcask) Sync() error {
	return b.curr.Sync()
}

// Get retrieves the value of the given key. If the key is not found or an/I/O
// error occurs a null byte slice is returend along with the error.
func (b *Bitcask) Get(key string) ([]byte, error) {
	var df *internal.Datafile

	item, ok := b.keydir.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	if item.FileID == b.curr.FileID() {
		df = b.curr
	} else {
		df = b.datafiles[item.FileID]
	}

	e, err := df.ReadAt(item.Offset, item.Size)
	if err != nil {
		return nil, err
	}

	checksum := crc32.ChecksumIEEE(e.Value)
	if checksum != e.Checksum {
		return nil, ErrChecksumFailed
	}

	return e.Value, nil
}

// Has returns true if the key exists in the database, false otherwise.
func (b *Bitcask) Has(key string) bool {
	_, ok := b.keydir.Get(key)
	return ok
}

// Put stores the key and value in the database.
func (b *Bitcask) Put(key string, value []byte) error {
	if len(key) > b.config.maxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > b.config.maxValueSize {
		return ErrValueTooLarge
	}

	offset, n, err := b.put(key, value)
	if err != nil {
		return err
	}

	item := b.keydir.Add(key, b.curr.FileID(), offset, n)
	b.trie.Add(key, item)

	return nil
}

// Delete deletes the named key. If the key doesn't exist or an I/O error
// occurs the error is returned.
func (b *Bitcask) Delete(key string) error {
	_, _, err := b.put(key, []byte{})
	if err != nil {
		return err
	}

	b.keydir.Delete(key)
	b.trie.Remove(key)

	return nil
}

// Scan performa a prefix scan of keys matching the given prefix and calling
// the function `f` with the keys found. If the function returns an error
// no further keys are processed and the first error returned.
func (b *Bitcask) Scan(prefix string, f func(key string) error) error {
	keys := b.trie.PrefixSearch(prefix)
	for _, key := range keys {
		if err := f(key); err != nil {
			return err
		}
	}
	return nil
}

// Len returns the total number of keys in the database
func (b *Bitcask) Len() int {
	return b.keydir.Len()
}

// Keys returns all keys in the database as a channel of string(s)
func (b *Bitcask) Keys() chan string {
	return b.keydir.Keys()
}

// Fold iterates over all keys in the database calling the function `f` for
// each key. If the function returns an error, no further keys are processed
// and the error returned.
func (b *Bitcask) Fold(f func(key string) error) error {
	for key := range b.keydir.Keys() {
		if err := f(key); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bitcask) put(key string, value []byte) (int64, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	size := b.curr.Size()
	if size >= int64(b.config.maxDatafileSize) {
		err := b.curr.Close()
		if err != nil {
			return -1, 0, err
		}

		df, err := internal.NewDatafile(b.path, b.curr.FileID(), true)
		if err != nil {
			return -1, 0, err
		}

		b.datafiles = append(b.datafiles, df)

		id := b.curr.FileID() + 1
		curr, err := internal.NewDatafile(b.path, id, false)
		if err != nil {
			return -1, 0, err
		}
		b.curr = curr
	}

	e := internal.NewEntry(key, value)
	return b.curr.Write(e)
}

// Merge merges all datafiles in the database creating hint files for faster
// startup. Old keys are squashed and deleted keys removes. Call this function
// periodically to reclaim disk space.
func Merge(path string, force bool) error {
	fns, err := internal.GetDatafiles(path)
	if err != nil {
		return err
	}

	ids, err := internal.ParseIds(fns)
	if err != nil {
		return err
	}

	// Do not merge if we only have 1 Datafile
	if len(ids) <= 1 {
		return nil
	}

	// Don't merge the Active Datafile (the last one)
	fns = fns[:len(fns)-1]
	ids = ids[:len(ids)-1]

	temp, err := ioutil.TempDir("", "bitcask")
	if err != nil {
		return err
	}

	for i, fn := range fns {
		// Don't merge Datafiles whose .hint files we've already generated
		// (they are already merged); unless we set the force flag to true
		// (forcing a re-merge).
		if filepath.Ext(fn) == ".hint" && !force {
			// Already merged
			continue
		}

		id := ids[i]

		keydir := internal.NewKeydir()

		df, err := internal.NewDatafile(path, id, true)
		if err != nil {
			return err
		}
		defer df.Close()

		for {
			e, n, err := df.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// Tombstone value  (deleted key)
			if len(e.Value) == 0 {
				keydir.Delete(e.Key)
				continue
			}

			keydir.Add(e.Key, ids[i], e.Offset, n)
		}

		tempdf, err := internal.NewDatafile(temp, id, false)
		if err != nil {
			return err
		}
		defer tempdf.Close()

		for key := range keydir.Keys() {
			item, _ := keydir.Get(key)
			e, err := df.ReadAt(item.Offset, item.Size)
			if err != nil {
				return err
			}

			_, _, err = tempdf.Write(e)
			if err != nil {
				return err
			}
		}

		err = tempdf.Close()
		if err != nil {
			return err
		}

		err = df.Close()
		if err != nil {
			return err
		}

		err = os.Rename(tempdf.Name(), df.Name())
		if err != nil {
			return err
		}

		hint := strings.TrimSuffix(df.Name(), ".data") + ".hint"
		err = keydir.Save(hint)
		if err != nil {
			return err
		}
	}

	return nil
}

// Open opens the database at the given path with optional options.
// Options can be provided with the `WithXXX` functions that provide
// configuration options as functions.
func Open(path string, options ...Option) (*Bitcask, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	err := Merge(path, false)
	if err != nil {
		return nil, err
	}

	fns, err := internal.GetDatafiles(path)
	if err != nil {
		return nil, err
	}

	ids, err := internal.ParseIds(fns)
	if err != nil {
		return nil, err
	}

	var datafiles []*internal.Datafile

	keydir := internal.NewKeydir()
	trie := trie.New()

	for i, fn := range fns {
		df, err := internal.NewDatafile(path, ids[i], true)
		if err != nil {
			return nil, err
		}
		datafiles = append(datafiles, df)

		if filepath.Ext(fn) == ".hint" {
			f, err := os.Open(filepath.Join(path, fn))
			if err != nil {
				return nil, err
			}
			defer f.Close()

			hint, err := internal.NewKeydirFromBytes(f)
			if err != nil {
				return nil, err
			}

			for key := range hint.Keys() {
				item, _ := hint.Get(key)
				_ = keydir.Add(key, item.FileID, item.Offset, item.Size)
				trie.Add(key, item)
			}
		} else {
			for {
				e, n, err := df.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}

				// Tombstone value  (deleted key)
				if len(e.Value) == 0 {
					keydir.Delete(e.Key)
					continue
				}

				item := keydir.Add(e.Key, ids[i], e.Offset, n)
				trie.Add(e.Key, item)
			}
		}
	}

	var id int
	if len(ids) > 0 {
		id = ids[(len(ids) - 1)]
	}

	curr, err := internal.NewDatafile(path, id, false)
	if err != nil {
		return nil, err
	}

	bitcask := &Bitcask{
		Flock:     flock.New(filepath.Join(path, "lock")),
		config:    newDefaultConfig(),
		path:      path,
		curr:      curr,
		keydir:    keydir,
		datafiles: datafiles,
		trie:      trie,
	}

	for _, opt := range options {
		err = opt(bitcask.config)
		if err != nil {
			return nil, err
		}
	}

	locked, err := bitcask.Flock.TryLock()
	if err != nil {
		return nil, err
	}

	if !locked {
		return nil, ErrDatabaseLocked
	}

	return bitcask, nil
}
