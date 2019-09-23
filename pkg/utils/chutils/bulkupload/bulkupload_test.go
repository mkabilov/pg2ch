package bulkupload

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"gopkg.in/djherbis/buffer.v1"

	"github.com/mkabilov/pg2ch/pkg/config"
)

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
	return nil, nil
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
		bUpl := New(chConn, 1024, gzip.NoCompression)
		if err := bUpl.Init(buffer.New(1024)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		doneCh := make(chan struct{})

		go func() {
			if err := bUpl.BulkUpload(config.ChTableName{DatabaseName: "default", TableName: "my_table"}, []string{"col1", "col2"}); err != nil {
				t.Fatalf("unexpected error while bulkuploading: %v", err)
			}
			doneCh <- struct{}{}
		}()

		for i := 0; i < 10; i++ {
			myChunk := []byte(fmt.Sprintf("ncomp%d", i))

			testData.Write(myChunk)
			if _, err := bUpl.Write(myChunk); err != nil {
				t.Fatalf("unexpected error while writing to bulk uploader: %v", err)
			}

			testData.WriteByte('\n')
			if err := bUpl.WriteByte('\n'); err != nil {
				t.Fatalf("unexpected error while writing to bulk uploader: %v", err)
			}
		}

		if err := bUpl.Finish(); err != nil {
			t.Fatalf("unexpected error while finishing bulk upload: %v", err)
		}

		<-doneCh

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
		bUpl := New(chConn, 1024, gzip.BestSpeed)
		if err := bUpl.Init(buffer.New(1024)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		doneCh := make(chan struct{})

		go func() {
			if err := bUpl.BulkUpload(config.ChTableName{DatabaseName: "default", TableName: "my_table"}, []string{"col1", "col2"}); err != nil {
				t.Fatalf("unexpected error while bulkuploading: %v", err)
			}
			doneCh <- struct{}{}
		}()

		for i := 0; i < 10; i++ {
			myChunk := []byte(fmt.Sprintf("comp%d", i))

			testData.Write(myChunk)
			if _, err := bUpl.Write(myChunk); err != nil {
				t.Fatalf("unexpected error while writing to bulk uploader: %v", err)
			}

			testData.WriteByte('\n')
			if err := bUpl.WriteByte('\n'); err != nil {
				t.Fatalf("unexpected error while writing to bulk uploader: %v", err)
			}
		}

		if err := bUpl.Finish(); err != nil {
			t.Fatalf("unexpected error while finishing bulk upload: %v", err)
		}

		<-doneCh

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
