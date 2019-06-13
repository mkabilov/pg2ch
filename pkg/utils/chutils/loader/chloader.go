package loader

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
)

type CHLoader struct {
	client        *http.Client
	baseURL       string
	urlValues     url.Values
	gzipWriter    *gzip.Writer
	requestBuffer *bytes.Buffer
}

func New(baseURL, dbName string) *CHLoader {
	var err error
	ch := &CHLoader{
		client:        &http.Client{},
		urlValues:     url.Values{},
		baseURL:       strings.TrimRight(baseURL, "/") + "/",
		requestBuffer: &bytes.Buffer{},
	}
	ch.gzipWriter, err = gzip.NewWriterLevel(ch.requestBuffer, gzip.BestSpeed)
	if err != nil {
		log.Fatalf("could not create gzip writer: %v", err)
	}
	ch.urlValues.Add("database", dbName)

	return ch
}

func insertQuery(tableName string, columns []string) string {
	columnsStr := ""
	queryFormat := "INSERT INTO %s%s FORMAT TabSeparated"
	if columns != nil && len(columns) > 0 {
		columnsStr = "(" + strings.Join(columns, ", ") + ")"
	}

	return fmt.Sprintf(queryFormat, tableName, columnsStr)
}

func (c *CHLoader) urlParams() url.Values {
	res := make(url.Values, len(url.Values{}))
	for k, v := range c.urlValues {
		res[k] = v
	}

	return res
}

func (c *CHLoader) performRequest(query string, reqBody io.Reader) error {
	vals := c.urlParams()
	vals.Add("query", query)

	req, err := http.NewRequest(http.MethodPost, c.baseURL+"?"+vals.Encode(), reqBody)
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

func (c *CHLoader) BufferWrite(p []byte) error {
	_, err := c.gzipWriter.Write(p)
	if err != nil {
		return err
	}

	return nil
}

func (c *CHLoader) BufferFlush(tableName string, columns []string) error {
	if err := c.gzipWriter.Close(); err != nil {
		return fmt.Errorf("could not close gzip writer: %v", err)
	}

	if err := c.performRequest(insertQuery(tableName, columns), c.requestBuffer); err != nil {
		return err
	}
	c.requestBuffer.Reset()

	c.gzipWriter.Reset(c.requestBuffer)

	return nil
}

func (c *CHLoader) Exec(query string) error {
	log.Printf("exec: %q", query)

	req, err := http.NewRequest(http.MethodPost, c.baseURL+"?"+c.urlParams().Encode(), bytes.NewBufferString(query))
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
	log.Printf("query: %q", query)
	res := make([][]string, 0)

	req, err := http.NewRequest(http.MethodPost, c.baseURL+"?"+c.urlParams().Encode(), bytes.NewBufferString(query))
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
