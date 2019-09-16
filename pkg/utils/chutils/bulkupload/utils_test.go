package bulkupload

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"

	"github.com/mkabilov/pg2ch/pkg/config"
)

var (
	decodeTuplesTest = []struct {
		row       []byte
		pgColumns map[int]*config.PgColumn
		colProps  map[int]*config.ColumnProperty
		expected  []byte
	}{
		{
			row: []byte("1\t2\t3\n"),
			pgColumns: map[int]*config.PgColumn{
				0: {Column: config.Column{BaseType: dbtypes.PgInteger}},
				1: {Column: config.Column{BaseType: dbtypes.PgInteger}},
				2: {Column: config.Column{BaseType: dbtypes.PgInteger}},
			},
			colProps: map[int]*config.ColumnProperty{
				0: {Coalesce: nil},
				1: {Coalesce: nil},
				2: {Coalesce: nil},
			},
			expected: []byte("1\t2\t3"),
		},
		{
			row: []byte("1\t\\N\t3\n"),
			pgColumns: map[int]*config.PgColumn{
				0: {Column: config.Column{BaseType: dbtypes.PgInteger}},
				1: {Column: config.Column{BaseType: dbtypes.PgInteger}},
				2: {Column: config.Column{BaseType: dbtypes.PgInteger}},
			},
			colProps: map[int]*config.ColumnProperty{
				0: {Coalesce: nil},
				1: {Coalesce: nil},
				2: {Coalesce: nil},
			},
			expected: []byte("1\t\\N\t3"),
		},
		{
			row: []byte("1\t\\N\t3\n"),
			pgColumns: map[int]*config.PgColumn{
				0: {Column: config.Column{BaseType: dbtypes.PgInteger}},
				2: {Column: config.Column{BaseType: dbtypes.PgInteger}},
			},
			colProps: map[int]*config.ColumnProperty{
				0: {Coalesce: nil},
				2: {Coalesce: nil},
			},
			expected: []byte("1\t3"),
		},
		{
			row: []byte(`\x48\x65\x6C\x6c\x6f` + "\n"),
			pgColumns: map[int]*config.PgColumn{
				0: {Column: config.Column{BaseType: dbtypes.PgText}},
			},
			colProps: map[int]*config.ColumnProperty{
				0: {Coalesce: nil},
			},
			expected: []byte("Hello"),
		},
		{
			row: []byte(`\110\145\154\154\157` + "\n"),
			pgColumns: map[int]*config.PgColumn{
				0: {Column: config.Column{BaseType: dbtypes.PgText}},
			},
			colProps: map[int]*config.ColumnProperty{
				0: {Coalesce: nil},
			},
			expected: []byte("Hello"),
		},
	}
)

type mockWriter struct {
	buf *bytes.Buffer
}

func (mw *mockWriter) Write(p []byte) (int, error) {
	return mw.buf.Write(p)
}

func (mw *mockWriter) WriteByte(p byte) error {
	_, err := mw.buf.Write([]byte{p})

	return err
}

func (mw *mockWriter) Reset() {
	mw.buf.Reset()
}

func TestDecodeCopyToTuples(t *testing.T) {
	w := &mockWriter{buf: &bytes.Buffer{}}
	buf := &bytes.Buffer{}

	for i, tt := range decodeTuplesTest {
		if err := DecodeCopyToTuples(buf, w, tt.pgColumns, tt.colProps, tt.row); err != nil {
			t.Fatalf("%d: unexpected error: %v", i, err)
		}

		got, err := ioutil.ReadAll(w.buf)
		if err != nil {
			t.Fatalf("%d: unexpected error while reading out buf: %v", i, err)
		}

		if bytes.Compare(got, tt.expected) != 0 {
			t.Fatalf("%d: expected %q, got %q", i, tt.expected, got)
		}
	}
}
