package bulkupload

import (
	"compress/flate"
	"compress/gzip"
	"fmt"

	"gopkg.in/djherbis/buffer.v1"
	"gopkg.in/djherbis/nio.v2"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

type BulkUploader interface {
	utils.Writer

	Init(buffer.BufferAt) error
	Finish() error
	BulkUpload(name config.ChTableName, columns []string) error
}

type BulkUpload struct {
	conn           *chutils.CHConn
	pipeWriter     *nio.PipeWriter
	pipeReader     *nio.PipeReader
	gzipWriter     *gzip.Writer
	tableName      string
	columns        []string
	useGzip        bool
	gzipComprLevel int
	gzipBufBytes   int
	gzipBufSize    int
}

func New(cfg *config.CHConnConfig, gzipBufSize int, comprLevel config.GzipComprLevel) *BulkUpload {
	ch := &BulkUpload{
		conn:           chutils.MakeChConnection(cfg),
		gzipBufSize:    gzipBufSize,
		gzipComprLevel: int(comprLevel),
		useGzip:        comprLevel != flate.NoCompression,
	}

	return ch
}

func (c *BulkUpload) BulkUpload(tableName config.ChTableName, columns []string) error {
	return c.conn.PerformInsert(tableName, columns, c.pipeReader)
}

//Prepare pipes
func (c *BulkUpload) Init(buf buffer.BufferAt) error {
	var err error

	c.pipeReader, c.pipeWriter = nio.Pipe(buf)
	if c.useGzip {
		c.gzipWriter, err = gzip.NewWriterLevel(c.pipeWriter, c.gzipComprLevel)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *BulkUpload) Write(p []byte) (int, error) {
	if !c.useGzip {
		return c.pipeWriter.Write(p)
	}

	c.gzipBufBytes += len(p)

	n, err := c.gzipWriter.Write(p)
	if c.gzipBufBytes >= c.gzipBufSize {
		if err := c.gzipWriter.Flush(); err != nil {
			return 0, fmt.Errorf("could not flush gzip: %v", err)
		}
		c.gzipBufBytes = 0
	}

	return n, err
}

func (c *BulkUpload) WriteByte(p byte) error {
	_, err := c.Write([]byte{p})
	return err
}

func (c *BulkUpload) Finish() error {
	if c.useGzip {
		if err := c.gzipWriter.Close(); err != nil {
			return err
		}
	}

	return c.pipeWriter.Close()
}
