package chutils

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

var (
	baseTypes = []string{
		dbtypes.PgAdjustAjBool,
		dbtypes.PgBoolean,

		dbtypes.PgCharacterVarying,
		dbtypes.PgCharacter,
		dbtypes.PgChar,
		dbtypes.PgText,
		dbtypes.PgVarchar,

		dbtypes.PgBigint,
		dbtypes.PgInteger,
		dbtypes.PgSmallint,
		dbtypes.PgNumeric,
		dbtypes.PgReal,
		dbtypes.PgDecimal,
		dbtypes.PgDoublePrecision,

		dbtypes.PgAdjustAjDate,
		dbtypes.PgAdjustAjTime,
		dbtypes.PgDate,
		dbtypes.PgTimestampWithTimeZone,
		dbtypes.PgTimestampWithoutTimeZone,
		dbtypes.PgTimestamp,
		dbtypes.PgTime,
		dbtypes.PgInterval,

		dbtypes.PgAdjustOsName,
		dbtypes.PgAdjustDeviceType,
		dbtypes.PgInet,
		dbtypes.PgJson,
		dbtypes.PgJsonb,
		dbtypes.PgUuid,
		dbtypes.PgAdjustBigIstore,
		dbtypes.PgAdjustIstore,
	}

	istoreTypes = map[string]struct{}{
		dbtypes.PgAdjustBigIstore: {},
		dbtypes.PgAdjustIstore:    {},
	}

	convertBaseTypeTestData = []struct {
		baseType string
		val      string
		exp      string
	}{
		{baseType: dbtypes.PgAdjustAjBool, val: `f`, exp: `0`},
		{baseType: dbtypes.PgAdjustAjBool, val: `t`, exp: `1`},
		{baseType: dbtypes.PgAdjustAjBool, val: `u`, exp: `-1`},
		{baseType: dbtypes.PgBoolean, val: `f`, exp: `0`},
		{baseType: dbtypes.PgBoolean, val: `t`, exp: `1`},

		{baseType: dbtypes.PgCharacterVarying, val: `foo`, exp: `foo`},
		{baseType: dbtypes.PgCharacterVarying, val: ``, exp: ``},
		{baseType: dbtypes.PgChar, val: `a`, exp: `a`},
		{baseType: dbtypes.PgChar, val: ``, exp: ``},
		{baseType: dbtypes.PgCharacter, val: `a`, exp: `a`},
		{baseType: dbtypes.PgCharacter, val: ``, exp: ``},
		{baseType: dbtypes.PgVarchar, val: `abc`, exp: `abc`},
		{baseType: dbtypes.PgVarchar, val: ``, exp: ``},
		{baseType: dbtypes.PgText, val: `abc`, exp: `abc`},
		{baseType: dbtypes.PgText, val: ``, exp: ``},
		{baseType: dbtypes.PgText, val: "\t123\nabc\t", exp: `\t123\nabc\t`},

		{baseType: dbtypes.PgBigint, val: `123`, exp: `123`},
		{baseType: dbtypes.PgBigint, val: `0`, exp: `0`},
		{baseType: dbtypes.PgBigint, val: `-123`, exp: `-123`},
		{baseType: dbtypes.PgInteger, val: `123`, exp: `123`},
		{baseType: dbtypes.PgInteger, val: `0`, exp: `0`},
		{baseType: dbtypes.PgInteger, val: `-123`, exp: `-123`},
		{baseType: dbtypes.PgSmallint, val: `123`, exp: `123`},
		{baseType: dbtypes.PgSmallint, val: `0`, exp: `0`},
		{baseType: dbtypes.PgSmallint, val: `-123`, exp: `-123`},
		{baseType: dbtypes.PgNumeric, val: `123.123`, exp: `123.123`},
		{baseType: dbtypes.PgNumeric, val: `123`, exp: `123`},
		{baseType: dbtypes.PgNumeric, val: `0`, exp: `0`},
		{baseType: dbtypes.PgReal, val: `123`, exp: `123`},
		{baseType: dbtypes.PgReal, val: `0`, exp: `0`},
		{baseType: dbtypes.PgReal, val: `123.432`, exp: `123.432`},
		{baseType: dbtypes.PgDecimal, val: `123.1`, exp: `123.1`},
		{baseType: dbtypes.PgDecimal, val: `0`, exp: `0`},
		{baseType: dbtypes.PgDecimal, val: `-123.1`, exp: `-123.1`},
		{baseType: dbtypes.PgDoublePrecision, val: `123.123`, exp: `123.123`},
		{baseType: dbtypes.PgDoublePrecision, val: `0`, exp: `0`},
		{baseType: dbtypes.PgDoublePrecision, val: `-123.123`, exp: `-123.123`},

		{baseType: dbtypes.PgAdjustAjDate, val: `2019-04-01`, exp: `2019-04-01`},
		{baseType: dbtypes.PgAdjustAjTime, val: `2019-04-01 01:15:04`, exp: `2019-04-01 01:15:04`},
		{baseType: dbtypes.PgDate, val: `2019-04-01`, exp: `2019-04-01`},

		{baseType: dbtypes.PgTimestampWithTimeZone, val: `2019-04-01 01:15:04.123+02`, exp: `2019-04-01 01:15:04`},
		{baseType: dbtypes.PgTimestampWithoutTimeZone, val: `2019-04-01 01:15:04.123`, exp: `2019-04-01 01:15:04`},
		{baseType: dbtypes.PgTimestamp, val: `2019-04-01 01:15:04`, exp: `2019-04-01 01:15:04`},
		{baseType: dbtypes.PgTime, val: `01:15:04.123`, exp: `01:15:04`},
		{baseType: dbtypes.PgTimeWithoutTimeZone, val: `01:15:04.333`, exp: `01:15:04`},
		{baseType: dbtypes.PgTimeWithTimeZone, val: `01:15:04.123+02`, exp: `01:15:04`},
		{baseType: dbtypes.PgInterval, val: `00:00:00`, exp: `0`},
		{baseType: dbtypes.PgInterval, val: `00:00:12`, exp: `12`},
		{baseType: dbtypes.PgInterval, val: `00:11:12`, exp: `672`},
		{baseType: dbtypes.PgInterval, val: `23:11:12`, exp: `83472`},

		{baseType: dbtypes.PgAdjustOsName, val: `abc`, exp: `abc`},
		{baseType: dbtypes.PgAdjustDeviceType, val: `abc`, exp: `abc`},
		{baseType: dbtypes.PgJson, val: `abc`, exp: `abc`},
		{baseType: dbtypes.PgJsonb, val: `abc`, exp: `abc`},
		{baseType: dbtypes.PgUuid, val: `abc`, exp: `abc`},

		{baseType: dbtypes.PgAdjustBigIstore, val: `"0"=>"10", "1"=>"20", "2"=>"30"`, exp: "[0,1,2]\t[10,20,30]"},
		{baseType: dbtypes.PgAdjustBigIstore, val: `"-321"=>"-100", "0"=>"0"`, exp: "[-321,0]\t[-100,0]"},
		{baseType: dbtypes.PgAdjustBigIstore, val: `"1"=>"2"`, exp: "[1]\t[2]"},
	}

	convertArrayTypeTestData = []struct {
		baseType string
		val      string
		exp      string
	}{
		{baseType: dbtypes.PgInteger, val: "{1,2,3,4,5}", exp: "[1,2,3,4,5]"},
		{baseType: dbtypes.PgInteger, val: "{1}", exp: "[1]"},
		{baseType: dbtypes.PgInteger, val: "{}", exp: "[]"},

		{baseType: dbtypes.PgBigint, val: "{1,2,3,4,5}", exp: "[1,2,3,4,5]"},
		{baseType: dbtypes.PgBigint, val: "{1}", exp: "[1]"},
		{baseType: dbtypes.PgBigint, val: "{}", exp: "[]"},

		{baseType: dbtypes.PgAdjustAjBool, val: `{f,t,t,u}`, exp: `[0,1,1,-1]`},
		{baseType: dbtypes.PgAdjustAjBool, val: `{}`, exp: `[]`},
		{baseType: dbtypes.PgBoolean, val: `{f}`, exp: `[0]`},
		{baseType: dbtypes.PgBoolean, val: `{t}`, exp: `[1]`},

		{baseType: dbtypes.PgInterval, val: `{00:00:00,00:00:12,00:11:12,23:11:12}`, exp: `[0,12,672,83472]`},
		{baseType: dbtypes.PgText, val: `{a,b,c}`, exp: `['a','b','c']`},
		{baseType: dbtypes.PgDate, val: `{2019-01-01}`, exp: `['2019-01-01']`},
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

func BenchmarkConvertBaseType(b *testing.B) {
	w := &bytes.Buffer{}
	tupleData := &message.Tuple{
		Kind:  message.TupleText,
		Value: []byte("Lorem ipsum dolor sit amet,\n consectetur adipiscing elit."),
	}

	for n := 0; n < b.N; n++ {
		convertBaseType(w, dbtypes.PgText, tupleData, &config.ColumnProperty{})
	}
}

func TestConvertBaseType(t *testing.T) {
	w := &mockWriter{buf: &bytes.Buffer{}}

	t.Run("nulls", func(t *testing.T) {
		for _, bType := range baseTypes {
			w.Reset()

			column := &config.PgColumn{
				Column: config.Column{
					BaseType:   bType,
					IsArray:    false,
					IsNullable: false,
					Ext:        nil,
				},
			}

			tupleData := &message.Tuple{
				Kind:  message.TupleNull,
				Value: nil,
			}

			if err := ConvertColumn(w, column, tupleData, &config.ColumnProperty{}); err != nil {
				t.Fatalf("%s: unexpected error: %v", bType, err)
			}

			buf, err := ioutil.ReadAll(w.buf)
			if err != nil {
				t.Fatalf("%s: unexpected error while reading the buffer: %v", bType, err)
			}

			if _, ok := istoreTypes[bType]; ok {
				if bytes.Compare(buf, []byte("[]\t[]")) != 0 {
					t.Fatalf("%s: exp []\t[], got %q", bType, string(buf))
				}
			} else {
				if bytes.Compare(buf, []byte(`\N`)) != 0 {
					t.Fatalf("%s: exp \\N, got %q", bType, string(buf))
				}
			}
		}
	})

	t.Run("nulls arrays", func(t *testing.T) {
		for _, bType := range baseTypes {
			w.Reset()

			column := &config.PgColumn{
				Column: config.Column{
					BaseType:   bType,
					IsArray:    true,
					IsNullable: false,
					Ext:        nil,
				},
			}

			tupleData := &message.Tuple{
				Kind:  message.TupleNull,
				Value: nil,
			}

			if err := ConvertColumn(w, column, tupleData, &config.ColumnProperty{}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			buf, err := ioutil.ReadAll(w.buf)
			if err != nil {
				t.Fatalf("unexpected error while reading the buffer: %v", err)
			}

			if bytes.Compare(buf, []byte(`[]`)) != 0 {
				t.Fatalf("%s: exp [], got %q", bType, string(buf))
			}
		}
	})

	t.Run("coalesce", func(t *testing.T) {
		for _, bType := range baseTypes {
			w.Reset()

			column := &config.PgColumn{
				Column: config.Column{
					BaseType: bType,
					IsArray:  false,
				},
			}

			tupleData := &message.Tuple{
				Kind:  message.TupleNull,
				Value: nil,
			}

			colProps := &config.ColumnProperty{
				Coalesce: []byte("foo"),
			}

			if err := ConvertColumn(w, column, tupleData, colProps); err != nil {
				t.Fatalf("%s: unexpected error: %v", bType, err)
			}

			buf, err := ioutil.ReadAll(w.buf)
			if err != nil {
				t.Fatalf("%s: unexpected error while reading the buffer: %v", bType, err)
			}

			if bytes.Compare(buf, colProps.Coalesce) != 0 {
				t.Fatalf("%s: exp %q, got %q", bType, string(colProps.Coalesce), string(buf))
			}
		}
	})

	t.Run("not null", func(t *testing.T) {
		for _, vv := range convertBaseTypeTestData {
			w.Reset()

			column := &config.PgColumn{
				Column: config.Column{
					BaseType: vv.baseType,
					IsArray:  false,
				},
			}

			tupleData := &message.Tuple{
				Kind:  message.TupleText,
				Value: []byte(vv.val),
			}

			if err := ConvertColumn(w, column, tupleData, &config.ColumnProperty{}); err != nil {
				t.Fatalf("%s:unexpected error: %v", vv.baseType, err)
			}

			buf, err := ioutil.ReadAll(w.buf)
			if err != nil {
				t.Fatalf("%s: unexpected error while reading the buffer: %v", vv.baseType, err)
			}

			if bytes.Compare(buf, []byte(vv.exp)) != 0 {
				t.Fatalf("%s: exp %q, got %q", vv.baseType, string(vv.exp), string(buf))
			}
		}
	})

	t.Run("arrays", func(t *testing.T) {
		for _, vv := range convertArrayTypeTestData {
			w.Reset()
			column := &config.PgColumn{
				Column: config.Column{
					BaseType: vv.baseType,
					IsArray:  true,
				},
			}

			tupleData := &message.Tuple{
				Kind:  message.TupleText,
				Value: []byte(vv.val),
			}

			if err := ConvertColumn(w, column, tupleData, &config.ColumnProperty{}); err != nil {
				t.Fatalf("%s array: unexpected error: %v", vv.baseType, err)
			}

			buf, err := ioutil.ReadAll(w.buf)
			if err != nil {
				t.Fatalf("%s array: unexpected error while reading the buffer: %v", vv.baseType, err)
			}

			if bytes.Compare(buf, []byte(vv.exp)) != 0 {
				t.Fatalf("%s array: exp %q, got %q", vv.baseType, string(vv.exp), string(buf))
			}
		}
	})
}
