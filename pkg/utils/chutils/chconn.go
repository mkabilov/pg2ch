package chutils

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/mkabilov/pg2ch/pkg/config"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// ClickHouse connection
type CHConn struct {
	baseURL    string
	withParams bool
	client     *http.Client
}

// Make new ClickHouse connection. For HTTP it just makes URL
func MakeChConnection(c *config.CHConnConfig) *CHConn {
	var baseURL string
	connStr := url.Values{}

	if len(c.User) > 0 {
		connStr.Add("user", c.User)
	}

	if len(c.Password) > 0 {
		connStr.Add("password", c.Password)
	}

	for param, value := range c.Params {
		connStr.Add(param, value)
	}

	if len(connStr) > 0 {
		baseURL = fmt.Sprintf("http://%s:%d?%s", c.Host, c.Port, connStr.Encode())
	} else {
		baseURL = fmt.Sprintf("http://%s:%d", c.Host, c.Port)
	}

	return &CHConn{
		baseURL:    baseURL,
		withParams: len(connStr) > 0,
		client:     &http.Client{},
	}
}

func (c *CHConn) queryURL(query string) string {
	if c.withParams {
		return c.baseURL + "&query=" + url.QueryEscape(query)
	} else {
		return c.baseURL + "?query=" + url.QueryEscape(query)
	}
}

func insertQuery(tableName config.ChTableName, columns []string) string {
	columnsStr := ""
	queryFormat := "INSERT INTO %s%s FORMAT TabSeparated"
	if columns != nil && len(columns) > 0 {
		columnsStr = "(" + strings.Join(columns, ", ") + ")"
	}

	return fmt.Sprintf(queryFormat, tableName, columnsStr)
}

// Make INSERT command, sends SQL command as query parameter
func (c *CHConn) PerformInsert(tableName config.ChTableName, columns []string,
	reqBody io.Reader) error {

	req, err := http.NewRequest(http.MethodPost,
		c.queryURL(insertQuery(tableName, columns)), reqBody)

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

// Make command without results, sends SQL command as body
func (c *CHConn) Exec(query string) error {
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

func (c *CHConn) Query(query string) ([][]string, error) {
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

	res := make([][]string, 0)
	for scanner.Scan() {
		res = append(res, strings.Split(scanner.Text(), "\t"))
	}

	return res, nil
}
