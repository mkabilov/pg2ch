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

	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

type BulkUpload struct {
	client       *http.Client
	baseURL      string
	urlValues    url.Values
	pipeWriter   *io.PipeWriter
	pipeReader   *io.PipeReader
	gzipWriter   *gzip.Writer
	tableName    string
	columns      []string
	gzipBufBytes int
	gzipBufSize  int
}

func New(baseURL, dbName string, gzipBufSize int) *BulkUpload {
	var err error
	ch := &BulkUpload{
		client:      &http.Client{},
		urlValues:   url.Values{},
		baseURL:     strings.TrimRight(baseURL, "/") + "/",
		gzipBufSize: gzipBufSize,
	}

	ch.urlValues.Add("database", dbName)
	ch.pipeReader, ch.pipeWriter = io.Pipe()
	ch.gzipWriter, err = gzip.NewWriterLevel(ch.pipeWriter, gzip.BestSpeed)

	if err != nil {
		log.Fatalf("could not init gzip: %v", err)
	}

	return ch
}

func (c *BulkUpload) urlParams() url.Values {
	res := make(url.Values, len(url.Values{}))
	for k, v := range c.urlValues {
		res[k] = v
	}

	return res
}

func (c *BulkUpload) performRequest(query string, body io.Reader) error {
	vals := c.urlParams()
	vals.Add("query", query)

	req, err := http.NewRequest(http.MethodPost, c.baseURL+"?"+vals.Encode(), body)
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

func (c *BulkUpload) BulkUpload(tableName string, columns []string) error {
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

	return c.pipeWriter.Close()
}
