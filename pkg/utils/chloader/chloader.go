package chloader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type CHLoader struct {
	client     *http.Client
	baseURL    string
	buf        *bytes.Buffer
	urlValues  url.Values
	colBuf     *bytes.Buffer
	pipeWriter *io.PipeWriter
	pipeReader *io.PipeReader
}

func New(baseURL, dbName string) *CHLoader {
	ch := &CHLoader{
		client:    &http.Client{},
		buf:       &bytes.Buffer{},
		urlValues: url.Values{},
		baseURL:   strings.TrimRight(baseURL, "/") + "/",
		colBuf:    &bytes.Buffer{},
	}
	ch.urlValues.Add("database", dbName)
	ch.pipeReader, ch.pipeWriter = io.Pipe()

	return ch
}

func (c *CHLoader) urlParams() url.Values {
	res := make(url.Values, len(url.Values{}))
	for k, v := range c.urlValues {
		res[k] = v
	}

	return res
}

func (c *CHLoader) PipeWrite(val []byte) error {
	_, err := c.pipeWriter.Write(val)

	return err
}

func (c *CHLoader) PipeFinishWriting() error {
	return c.pipeWriter.Close()
}

func (c *CHLoader) BulkUpload(tableName string, columns []string) error {
	vals := c.urlParams()
	vals.Add("query", c.generateQuery(tableName, columns))

	urlStr := c.baseURL + "?" + vals.Encode()
	req, err := http.NewRequest(http.MethodPost, urlStr, c.pipeReader)
	if err != nil {
		return fmt.Errorf("could not create request: %v", err)
	}
	req.Header.Add("Content-Encoding", "br")

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

func (c *CHLoader) WriteLine(val []byte) {
	c.buf.Write(val)
	c.buf.WriteByte('\n')
}

func (c *CHLoader) generateQuery(tableName string, columns []string) string {
	columnsStr := ""
	queryFormat := "INSERT INTO %s%s FORMAT TabSeparated"
	if columns != nil && len(columns) > 0 {
		columnsStr = "(" + strings.Join(columns, ", ") + ")"
	}

	return fmt.Sprintf(queryFormat, tableName, columnsStr)
}

func (c *CHLoader) Upload(tableName string, columns []string) error {
	defer c.buf.Reset()

	vals := c.urlParams()
	vals.Add("query", c.generateQuery(tableName, columns))

	urlStr := c.baseURL + "?" + vals.Encode()
	req, err := http.NewRequest(http.MethodPost, urlStr, c.buf)
	if err != nil {
		return fmt.Errorf("could not create request: %v", err)
	}
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

func (c *CHLoader) Exec(query string) error {
	req, err := http.NewRequest(http.MethodPost, c.baseURL, bytes.NewBufferString(query))

	if err != nil {
		return fmt.Errorf("could not create request: %v", err)
	}
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

func (c *CHLoader) Query(query string) ([][]string, error) {
	res := make([][]string, 0)

	req, err := http.NewRequest(http.MethodPost, c.baseURL, bytes.NewBufferString(query))

	if err != nil {
		return nil, fmt.Errorf("could not create request: %v", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not perform request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("could not read err body: %v", err)
		}

		return nil, fmt.Errorf("got %d status code from clickhouse: %s", resp.StatusCode, string(body))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		res = append(res, strings.Split(scanner.Text(), "\t"))
	}

	return res, nil
}

func (c *CHLoader) Write(p []byte) (int, error) {
	return c.pipeWriter.Write(p)
}
