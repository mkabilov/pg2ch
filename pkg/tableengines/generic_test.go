package tableengines

import (
	"bytes"
	"testing"
)

var testData = []struct {
	str      []byte
	fields   [][]byte
	expected []byte
}{
	{str: []byte{}, fields: [][]byte{[]byte("abc")}, expected: []byte("abc")},
	{str: []byte{}, fields: [][]byte{[]byte("123"), []byte("456")}, expected: []byte("123\t456")},
	{str: []byte("abc"), fields: [][]byte{}, expected: []byte("abc")},
	{str: []byte("abc"), fields: [][]byte{[]byte("123")}, expected: []byte("abc\t123")},
	{str: []byte("abc"), fields: [][]byte{[]byte("123"), []byte("456")}, expected: []byte("abc\t123\t456")},
}

func TestAppendField(t *testing.T) {
	for i, tt := range testData {
		got := appendField(tt.str, tt.fields...)
		if bytes.Compare(tt.expected, got) != 0 {
			t.Fatalf("%d: Expected %v(%q), got %v(%q)", i, tt.expected, string(tt.expected), got, string(got))
		}
	}
}
