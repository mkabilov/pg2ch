package bulkupload

import (
	"compress/gzip"
	"fmt"

	"gopkg.in/djherbis/buffer.v1"
	"gopkg.in/djherbis/nio.v2"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

type BulkUploader interface {
	Init(buffer.BufferAt) error
	Finish() error
	Write(p []byte) error
	BulkUpload(name config.ChTableName, columns []string) error
}

type BulkUpload struct {
	conn         *chutils.CHConn
	pipeWriter   *nio.PipeWriter
	pipeReader   *nio.PipeReader
	gzipWriter   *gzip.Writer
	tableName    string
	columns      []string
	gzipBufBytes int
	gzipBufSize  int
}

func New(conn *chutils.CHConn, gzipBufSize int) *BulkUpload {
	ch := &BulkUpload{
		conn:        conn,
		gzipBufSize: gzipBufSize,
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
	c.gzipWriter, err = gzip.NewWriterLevel(c.pipeWriter, gzip.BestSpeed) // TODO: move gzip level to config
	if err != nil {
		return err
	}

	return nil
}

func (c *BulkUpload) Write(p []byte) error {
	c.gzipBufBytes += len(p)

	_, err := c.gzipWriter.Write(p)

	if c.gzipBufBytes >= c.gzipBufSize {
		if err := c.gzipWriter.Flush(); err != nil {
			return fmt.Errorf("could not flush gzip: %v", err)
		}
		c.gzipBufBytes = 0
	}

	return err
}

func (c *BulkUpload) Finish() error {
	if err := c.gzipWriter.Close(); err != nil {
		return err
	}

	return c.pipeWriter.Close()
}
