package bulkupload

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return buffer.New(100 * 1024 * 1024 * 1024)
	}}

type BulkUpload struct {
	client       *http.Client
	baseURL      string
	pipeWriter   *nio.PipeWriter
	pipeReader   *nio.PipeReader
	gzipWriter   *gzip.Writer
	buf          buffer.Buffer
	tableName    string
	columns      []string
	gzipBufBytes int
	gzipBufSize  int
}

func New(baseURL string, gzipBufSize int) *BulkUpload {
	var err error
	ch := &BulkUpload{
		client:      &http.Client{},
		baseURL:     strings.TrimRight(baseURL, "/") + "/",
		gzipBufSize: gzipBufSize,
		buf:         bufPool.Get().(buffer.Buffer),
	}
	ch.pipeReader, ch.pipeWriter = nio.Pipe(ch.buf)
	ch.gzipWriter, err = gzip.NewWriterLevel(ch.pipeWriter, gzip.BestSpeed)

	if err != nil {
		log.Fatalf("could not init gzip: %v", err)
	}

	return ch
}

func (c *BulkUpload) performRequest(query string, body io.Reader) error {
	req, err := http.NewRequest(http.MethodPost, c.baseURL+"?query="+url.QueryEscape(query), body)
	if err != nil {
		return fmt.Errorf("could not create request: %v", err)
	}
	req.Header.Add("Content-Encoding", "gzip")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("could not perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("could not read err body: %v", err)
		}

		return fmt.Errorf("got %d status code from clickhouse: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (c *BulkUpload) BulkUpload(tableName config.ChTableName, columns []string) error {
	if err := c.performRequest(chutils.InsertQuery(tableName, columns), c.pipeReader); err != nil {
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

func (c *BulkUpload) PipeFinishWriting() error {
	if err := c.gzipWriter.Close(); err != nil {
		return err
	}

	err := c.pipeWriter.Close()

	bufPool.Put(c.buf)

	return err
}
