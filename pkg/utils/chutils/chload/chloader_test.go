package chload

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/mkabilov/pg2ch/pkg/config"
	"io"
	"io/ioutil"
	"testing"
)

var testDataMap = make(map[int][]byte)

type chConnMock struct {
	buf *bytes.Buffer
}

func (c *chConnMock) Flush(tableName config.ChTableName, columns []string) error {
	return nil
}

func (c *chConnMock) Exec(str string) error {
	return nil
}

func (c *chConnMock) Query(string) ([][]string, error) {
	return [][]string{
		{"col1", "col2"},
		{"val1", "val2"},
	}, nil
}

func (c *chConnMock) Write(p []byte) (int, error) {
	return c.buf.Write(p)
}

func (c *chConnMock) WriteByte(p byte) error {
	_, err := c.buf.Write([]byte{p})

	return err
}

func (c *chConnMock) Reset() {
	c.buf.Reset()
}

func (c *chConnMock) PerformInsert(tblName config.ChTableName, columns []string, r io.Reader) error {
	p, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	c.buf.Write(p)
	return nil
}

func TestBulkLoader(t *testing.T) {
	testData := &bytes.Buffer{}
	chConn := &chConnMock{
		buf: &bytes.Buffer{},
	}

	t.Run("no compression", func(t *testing.T) {
		testData.Reset()
		chL := New(chConn, gzip.NoCompression)

		for i := 0; i < 10; i++ {
			myChunk := []byte(fmt.Sprintf("ncomp%d", i))

			testData.Write(myChunk)
			if _, err := chL.Write(myChunk); err != nil {
				t.Fatalf("unexpected error while writing to bulk uploader: %v", err)
			}

			testData.WriteByte('\n')
			if err := chL.WriteByte('\n'); err != nil {
				t.Fatalf("unexpected error while writing to bulk uploader: %v", err)
			}
		}

		if err := chL.Flush(config.ChTableName{DatabaseName: "", TableName: ""}, []string{}); err != nil {
			t.Fatalf("unexpected error while flushing ch loader: %v", err)
		}

		chConnBuf, err := ioutil.ReadAll(chConn.buf)
		if err != nil {
			t.Fatalf("unexpected error while reading ch connector buffer: %v", err)
		}

		if exp := testData.Bytes(); bytes.Compare(chConnBuf, exp) != 0 {
			t.Fatalf("expected: %q, got: %q", exp, chConnBuf)
		}

		chConn.Reset()
	})

	t.Run("with compression", func(t *testing.T) {
		testData.Reset()
		chL := New(chConn, gzip.BestSpeed)

		for i := 0; i < 10; i++ {
			myChunk := []byte(fmt.Sprintf("ncomp%d", i))

			testData.Write(myChunk)
			if _, err := chL.Write(myChunk); err != nil {
				t.Fatalf("unexpected error while writing to bulk uploader: %v", err)
			}

			testData.WriteByte('\n')
			if err := chL.WriteByte('\n'); err != nil {
				t.Fatalf("unexpected error while writing to bulk uploader: %v", err)
			}
		}

		if err := chL.Flush(config.ChTableName{DatabaseName: "", TableName: ""}, []string{}); err != nil {
			t.Fatalf("unexpected error while flushing ch loader: %v", err)
		}

		gzR, err := gzip.NewReader(chConn.buf)
		if err != nil {
			t.Fatalf("unexpected error while creating gzip reader: %v", err)
		}

		chConnBuf, err := ioutil.ReadAll(gzR)
		if err != nil {
			t.Fatalf("unexpected error while reading ch connector buffer: %v", err)
		}

		if exp := testData.Bytes(); bytes.Compare(chConnBuf, exp) != 0 {
			t.Fatalf("expected: %q, got: %q", exp, chConnBuf)
		}

		chConn.Reset()
	})
}

func init() {
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			testDataMap[i] = []byte(fmt.Sprintf("some %d test data: %d", i+1, i))
		} else {
			testDataMap[i] = []byte(fmt.Sprintf("%d", i))
		}
	}
}

func BenchmarkCHLoadWriteDefault(b *testing.B)         { benchmarkCHLoadWrite(gzip.DefaultCompression, b) }
func BenchmarkCHLoadWriteNoCompression(b *testing.B)   { benchmarkCHLoadWrite(gzip.NoCompression, b) }
func BenchmarkCHLoadWriteBestSpeed(b *testing.B)       { benchmarkCHLoadWrite(gzip.BestSpeed, b) }
func BenchmarkCHLoadWriteBestCompression(b *testing.B) { benchmarkCHLoadWrite(gzip.BestCompression, b) }
func BenchmarkCHLoadWriteHuffmanOnly(b *testing.B)     { benchmarkCHLoadWrite(gzip.HuffmanOnly, b) }

func benchmarkCHLoadWrite(comprLevel config.GzipComprLevel, b *testing.B) {
	chConn := &chConnMock{
		buf: &bytes.Buffer{},
	}

	chL := New(chConn, comprLevel)
	for n := 0; n < b.N; n++ {
		for i := 0; i < 100; i++ {
			chL.Write(testDataMap[n%10])
			if n%10 == 0 {
				chL.WriteByte('\n')
			} else {
				chL.WriteByte('\t')
			}
		}

		chL.Flush(config.ChTableName{DatabaseName: "", TableName: ""}, []string{})
	}
}
