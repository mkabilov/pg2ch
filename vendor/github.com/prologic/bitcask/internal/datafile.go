package internal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"

	pb "github.com/prologic/bitcask/internal/proto"
	"github.com/prologic/bitcask/internal/streampb"
)

const (
	DefaultDatafileFilename = "%09d.data"
)

var (
	ErrReadonly  = errors.New("error: read only datafile")
	ErrReadError = errors.New("error: read error")
)

type Datafile struct {
	sync.RWMutex

	id     int
	r      *os.File
	ra     *mmap.ReaderAt
	w      *os.File
	offset int64
	dec    *streampb.Decoder
	enc    *streampb.Encoder
}

func NewDatafile(path string, id int, readonly bool) (*Datafile, error) {
	var (
		r   *os.File
		ra  *mmap.ReaderAt
		w   *os.File
		err error
	)

	fn := filepath.Join(path, fmt.Sprintf(DefaultDatafileFilename, id))

	if !readonly {
		w, err = os.OpenFile(fn, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
		if err != nil {
			return nil, err
		}
	}

	r, err = os.Open(fn)
	if err != nil {
		return nil, err
	}
	stat, err := r.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "error calling Stat()")
	}

	ra, err = mmap.Open(fn)
	if err != nil {
		return nil, err
	}

	offset := stat.Size()

	dec := streampb.NewDecoder(r)
	enc := streampb.NewEncoder(w)

	return &Datafile{
		id:     id,
		r:      r,
		ra:     ra,
		w:      w,
		offset: offset,
		dec:    dec,
		enc:    enc,
	}, nil
}

func (df *Datafile) FileID() int {
	return df.id
}

func (df *Datafile) Name() string {
	return df.r.Name()
}

func (df *Datafile) Close() error {
	if df.w == nil {
		err := df.ra.Close()
		if err != nil {
			return err
		}
		return df.r.Close()
	}

	err := df.Sync()
	if err != nil {
		return err
	}
	return df.w.Close()
}

func (df *Datafile) Sync() error {
	if df.w == nil {
		return nil
	}
	return df.w.Sync()
}

func (df *Datafile) Size() int64 {
	df.RLock()
	defer df.RUnlock()
	return df.offset
}

func (df *Datafile) Read() (e pb.Entry, n int64, err error) {
	df.Lock()
	defer df.Unlock()

	n, err = df.dec.Decode(&e)
	if err != nil {
		return
	}

	return
}

func (df *Datafile) ReadAt(index, size int64) (e pb.Entry, err error) {
	var n int

	b := make([]byte, size)

	if df.w == nil {
		n, err = df.ra.ReadAt(b, index)
	} else {
		n, err = df.r.ReadAt(b, index)
	}
	if err != nil {
		return
	}
	if int64(n) != size {
		err = ErrReadError
		return
	}

	buf := bytes.NewBuffer(b)
	dec := streampb.NewDecoder(buf)
	_, err = dec.Decode(&e)
	return
}

func (df *Datafile) Write(e pb.Entry) (int64, int64, error) {
	if df.w == nil {
		return -1, 0, ErrReadonly
	}

	df.Lock()
	defer df.Unlock()

	e.Offset = df.offset

	n, err := df.enc.Encode(&e)
	if err != nil {
		return -1, 0, err
	}
	df.offset += n

	return e.Offset, n, nil
}
