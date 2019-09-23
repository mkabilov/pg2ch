package chload

import (
	"bytes"
	"net/http"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

type CHLoad struct {
	*bytes.Buffer
	client *http.Client
	conn   chutils.CHConnector
}

type CHLoader interface {
	utils.Writer

	Flush(tableName config.ChTableName, columns []string) error
	Exec(string) error
	Query(string) ([][]string, error)
}

func New(chConn chutils.CHConnector, gzipCompressionLevel config.GzipComprLevel) *CHLoad {
	ch := &CHLoad{
		conn:   chConn,
		Buffer: &bytes.Buffer{},
	}

	return ch
}

func (c *CHLoad) Flush(tableName config.ChTableName, columns []string) error {
	defer c.Buffer.Reset()

	if err := c.conn.PerformInsert(tableName, columns, c.Buffer); err != nil {
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
