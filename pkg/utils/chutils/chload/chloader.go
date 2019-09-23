package chload

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

const maxBufferCapacity = 1024 * 1024 * 1024

type CHLoad struct {
	*bytes.Buffer
	client    *http.Client
	conn      chutils.CHConnector
	useGzip   bool
	gzipBuff  *bytes.Buffer
	gzipLevel int
	gzipWr    *gzip.Writer
}

type CHLoader interface {
	utils.Writer

	Flush(tableName config.ChTableName, columns []string) error
	Exec(string) error
	Query(string) ([][]string, error)
}

func New(chConn chutils.CHConnector, gzipCompressionLevel config.GzipComprLevel) *CHLoad {
	var err error
	ch := &CHLoad{
		useGzip:  gzipCompressionLevel.UseCompression(),
		conn:     chConn,
		Buffer:   &bytes.Buffer{},
		gzipBuff: &bytes.Buffer{},
	}

	ch.gzipWr, err = gzip.NewWriterLevel(ch.gzipBuff, int(gzipCompressionLevel))
	if err != nil {
		panic(err)
	}

	return ch
}

func (c *CHLoad) Flush(tableName config.ChTableName, columns []string) error {
	defer c.Buffer.Reset()

	if !c.useGzip {
		if err := c.conn.PerformInsert(tableName, columns, c.Buffer); err != nil {
			return err
		}

		return nil
	}

	if _, err := c.gzipWr.Write(c.Buffer.Bytes()); err != nil {
		return err
	}

	if err := c.gzipWr.Close(); err != nil {
		return fmt.Errorf("could not close gzip writer: %v", err)
	}

	if err := c.conn.PerformInsert(tableName, columns, c.gzipBuff); err != nil {
		return err
	}

	return nil
}

func (c *CHLoad) Exec(query string) error {
	return c.conn.Exec(query)
}

func (c *CHLoad) Query(query string) ([][]string, error) {
	return c.conn.Query(query)
}
