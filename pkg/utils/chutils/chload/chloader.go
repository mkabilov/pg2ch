package chload

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

	"github.com/mkabilov/pg2ch/pkg/config"
)

type CHLoad struct {
	client        *http.Client
	baseURL       string
	urlValues     url.Values
	gzipWriter    *gzip.Writer
	requestBuffer *bytes.Buffer
}

type CHLoader interface {
	BufferWriteLine([]byte) error
	BufferFlush(tableName config.ChTableName, columns []string) error
	Exec(string) error
	Query(string) ([][]string, error)
}

func New(baseURL string) *CHLoad {
	var err error
	ch := &CHLoad{
		client:        &http.Client{},
		urlValues:     url.Values{},
		baseURL:       strings.TrimRight(baseURL, "/") + "/",
		requestBuffer: &bytes.Buffer{},
	}
	ch.gzipWriter, err = gzip.NewWriterLevel(ch.requestBuffer, gzip.BestSpeed)
	if err != nil {
		log.Fatalf("could not create gzip writer: %v", err)
	}

	return ch
}

func insertQuery(tableName config.ChTableName, columns []string) string {
	columnsStr := ""
	queryFormat := "INSERT INTO %s%s FORMAT TabSeparated"
	if columns != nil && len(columns) > 0 {
		columnsStr = "(" + strings.Join(columns, ", ") + ")"
	}

	return fmt.Sprintf(queryFormat, tableName, columnsStr)
}

func (c *CHLoad) performRequest(query string, reqBody io.Reader) error {
	req, err := http.NewRequest(http.MethodPost, c.baseURL+"?query="+url.QueryEscape(query), reqBody)
	if err != nil {
		return fmt.Errorf("could not create request: %v", err)
	}
	req.Header.Add("Content-Encoding", "gzip")
	req.Header.Set("User-Agent", config.ApplicationName)

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

	if err := c.performRequest(insertQuery(tableName, columns), c.requestBuffer); err != nil {
		return err
	}
	c.requestBuffer.Reset()

	c.gzipWriter.Reset(c.requestBuffer)

	return nil
}

func (c *CHLoad) Exec(query string) error {
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

func (c *CHLoad) Query(query string) ([][]string, error) {
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
