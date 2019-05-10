package streampb

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

const (
	// prefixSize is the number of bytes we preallocate for storing
	// our big endian lenth prefix buffer.
	prefixSize = 8
)

// NewEncoder creates a streaming protobuf encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: bufio.NewWriter(w)}
}

// Encoder wraps an underlying io.Writer and allows you to stream
// proto encodings on it.
type Encoder struct {
	w *bufio.Writer
}

// Encode takes any proto.Message and streams it to the underlying writer.
// Messages are framed with a length prefix.
func (e *Encoder) Encode(msg proto.Message) (int64, error) {
	prefixBuf := make([]byte, prefixSize)

	buf, err := proto.Marshal(msg)
	if err != nil {
		return 0, err
	}
	binary.BigEndian.PutUint64(prefixBuf, uint64(len(buf)))

	if _, err := e.w.Write(prefixBuf); err != nil {
		return 0, errors.Wrap(err, "failed writing length prefix")
	}

	n, err := e.w.Write(buf)
	if err != nil {
		return 0, errors.Wrap(err, "failed writing marshaled data")
	}

	if err = e.w.Flush(); err != nil {
		return 0, errors.Wrap(err, "failed flushing data")
	}

	return int64(n + prefixSize), nil
}

// NewDecoder creates a streaming protobuf decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// Decoder wraps an underlying io.Reader and allows you to stream
// proto decodings on it.
type Decoder struct {
	r io.Reader
}

// Decode takes a proto.Message and unmarshals the next payload in the
// underlying io.Reader. It returns an EOF when it's done.
func (d *Decoder) Decode(v proto.Message) (int64, error) {
	prefixBuf := make([]byte, prefixSize)

	_, err := io.ReadFull(d.r, prefixBuf)
	if err != nil {
		return 0, err
	}

	n := binary.BigEndian.Uint64(prefixBuf)

	buf := make([]byte, n)

	idx := uint64(0)
	for idx < n {
		m, err := d.r.Read(buf[idx:n])
		if err != nil {
			return 0, errors.Wrap(translateError(err), "failed reading marshaled data")
		}
		idx += uint64(m)
	}
	return int64(idx + prefixSize), proto.Unmarshal(buf[:n], v)
}

func translateError(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
