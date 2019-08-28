package chload

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"log"
	"net/http"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

type CHLoad struct {
	client        *http.Client
	conn          *chutils.CHConn
	useGzip       bool
	gzipWriter    *gzip.Writer
	requestBuffer *bytes.Buffer
}

type CHLoader interface {
	utils.Writer

	Flush(tableName config.ChTableName, columns []string) error
	Exec(string) error
	Query(string) ([][]string, error)
}

func New(cfg *config.CHConnConfig, gzipCompressionLevel config.GzipComprLevel) *CHLoad {
	var err error

	ch := &CHLoad{
		useGzip:       gzipCompressionLevel != gzip.NoCompression,
		conn:          chutils.MakeChConnection(cfg),
		requestBuffer: &bytes.Buffer{},
	}

	if ch.useGzip {
		ch.gzipWriter, err = gzip.NewWriterLevel(ch.requestBuffer, int(gzipCompressionLevel))
		if err != nil {
			log.Fatalf("could not create gzip writer: %v", err)
		}
	}

	return ch
}

func (c *CHLoad) Write(p []byte) (int, error) {
	if !c.useGzip {
		return c.requestBuffer.Write(p)
	}

	n, err := c.gzipWriter.Write(p)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (c *CHLoad) WriteByte(p byte) error {
	if !c.useGzip {
		return c.requestBuffer.WriteByte(p)
	}

	// Create buffer
	_, err := c.gzipWriter.Write([]byte{p})
	if err != nil {
		return err
	}

	return nil
}

func (c *CHLoad) Flush(tableName config.ChTableName, columns []string) error {
	if !c.useGzip {
		if err := c.conn.PerformInsert(tableName, columns, c.requestBuffer); err != nil {
			return err
		}

		c.requestBuffer.Reset()
		return nil
	}

	if err := c.gzipWriter.Close(); err != nil {
		return fmt.Errorf("could not close gzip writer: %v", err)
	}

	if err := c.conn.PerformInsert(tableName, columns, c.requestBuffer); err != nil {
		return err
	}

	c.requestBuffer.Reset()
	c.gzipWriter.Reset(c.requestBuffer)

	return nil
}

func (c *CHLoad) Exec(query string) error {
	return c.conn.Exec(query)
}

func (c *CHLoad) Query(query string) ([][]string, error) {
	return c.conn.Query(query)
}
