package pgutils

import (
	"bytes"
	"testing"

	"github.com/mkabilov/pg2ch/pkg/message"
)

var (
	istoreToArrayTest = []struct {
		istore   []byte
		expected []byte
	}{
		{
			istore:   []byte{},
			expected: []byte("[]\t[]"),
		},
		{
			istore:   []byte(`"0"=>"2", "1"=>"1", "6"=>"1", "7"=>"1", "8"=>"2", "9"=>"1", "10"=>"2", "11"=>"1", "12"=>"1", "13"=>"1", "14"=>"2", "15"=>"2", "16"=>"3", "17"=>"2", "18"=>"3", "19"=>"1", "20"=>"1", "21"=>"2", "22"=>"2"`),
			expected: []byte("[0,1,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]\t[2,1,1,1,2,1,2,1,1,1,2,2,3,2,3,1,1,2,2]"),
		},
		{
			istore:   []byte(`"0"=>"2"`),
			expected: []byte("[0]\t[2]"),
		},
		{
			istore:   []byte(`"-1"=>"-2"`),
			expected: []byte("[-1]\t[-2]"),
		},
	}

	decodeTuplesTest = []struct {
		row      []byte
		expected message.Row
	}{
		{
			row: []byte("1\t2\t3\n"),
			expected: message.Row{
				&message.Tuple{Kind: message.TupleText, Value: []byte("1")},
				&message.Tuple{Kind: message.TupleText, Value: []byte("2")},
				&message.Tuple{Kind: message.TupleText, Value: []byte("3")},
			},
		},
		{
			row: []byte("1\t\\N\t3\n"),
			expected: message.Row{
				&message.Tuple{Kind: message.TupleText, Value: []byte("1")},
				&message.Tuple{Kind: message.TupleNull, Value: []byte{}},
				&message.Tuple{Kind: message.TupleText, Value: []byte("3")},
			},
		},
		{
			row:      []byte{},
			expected: message.Row{},
		},
		{
			row: []byte("\n"),
			expected: message.Row{
				&message.Tuple{Kind: message.TupleText, Value: []byte{}},
			},
		},
		{
			row: []byte(`\x48\x65\x6C\x6c\x6f` + "\n"),
			expected: message.Row{
				&message.Tuple{Kind: message.TupleText, Value: []byte("Hello")},
			},
		},
	}
)

func TestIstoreToArrays(t *testing.T) {
	for i, tt := range istoreToArrayTest {
		got := IstoreToArrays(tt.istore)
		if bytes.Compare(got, tt.expected) != 0 {
			t.Fatalf("%d: Expected: %v, got: %v", i, tt.expected, got)
		}
	}
}

func BenchmarkIstoreToArrays(b *testing.B) {
	for n := 0; n < b.N; n++ {
		IstoreToArrays(istoreToArrayTest[1].istore)
	}
}

func TestDecodeCopyToTuples(t *testing.T) {
	for i, tt := range decodeTuplesTest {
		got, err := DecodeCopyToTuples(tt.row)
		if err != nil {
			t.Fatalf("%d: Unexpected error: %v", i, err)
		}

		if len(got) != len(tt.expected) {
			t.Fatalf("%d: Expected %d columns, got %d columns", i, len(tt.expected), len(got))
		}

		for colId, colVal := range got {
			if bytes.Compare(colVal.Value, tt.expected[colId].Value) != 0 {
				t.Fatalf("%d: Expected Value %v, got %v", i, tt.expected[colId].Value, colVal.Value)
			}

			if colVal.Kind != tt.expected[colId].Kind {
				t.Fatalf("%d: Expected Kind %v, got %v", i, tt.expected[colId].Kind, colVal.Kind)
			}
		}
	}
}
