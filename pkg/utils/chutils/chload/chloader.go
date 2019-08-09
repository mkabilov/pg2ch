package chload

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"log"
	"net/http"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

type CHLoad struct {
	client        *http.Client
	conn          *chutils.CHConn
	gzipWriter    *gzip.Writer
	requestBuffer *bytes.Buffer
}

type CHLoader interface {
	BufferWriteLine([]byte) error
	BufferFlush(tableName config.ChTableName, columns []string) error
	Exec(string) error
	Query(string) ([][]string, error)
}

func New(conn *chutils.CHConn) *CHLoad {
	var err error
	ch := &CHLoad{
		conn:          conn,
		requestBuffer: &bytes.Buffer{},
	}
	ch.gzipWriter, err = gzip.NewWriterLevel(ch.requestBuffer, gzip.BestSpeed)
	if err != nil {
		log.Fatalf("could not create gzip writer: %v", err)
	}

	return ch
}

func (c *CHLoad) BufferWriteLine(p []byte) error {
	if _, err := c.gzipWriter.Write(p); err != nil {
		return err
	}

	if _, err := c.gzipWriter.Write([]byte("\n")); err != nil {
		return err
	}

	return nil
}

func (c *CHLoad) BufferFlush(tableName config.ChTableName, columns []string) error {
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
