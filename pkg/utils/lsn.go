package utils

import (
	"fmt"

	"github.com/jackc/pgx"
)

type LSN uint64

const (
	InvalidLSN LSN = 0
	hexFmt         = "%016x"
)

func (l LSN) Hex() string {
	return fmt.Sprintf(hexFmt, uint64(l))
}

func (l LSN) String() string {
	return pgx.FormatLSN(uint64(l))
}

func (l *LSN) ParseHex(hexStr string) error {
	var lsn LSN

	if n, err := fmt.Sscanf(hexStr, hexFmt, &lsn); err != nil {
		return fmt.Errorf("could not parse hex: %v", err)
	} else if n != 1 {
		return fmt.Errorf("could not parse hex")
	}

	*l = lsn

	return nil
}

func (l *LSN) Parse(lsn string) error {
	tmp, err := pgx.ParseLSN(lsn)
	if err != nil {
		return err

	}
	*l = (LSN)(tmp)
	return nil
}

func (l LSN) IsValid() bool {
	return l != InvalidLSN
}

func (l LSN) MarshalYAML() (interface{}, error) {
	return l.String(), nil
}

func (l *LSN) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var (
		lsn LSN
		val string
	)

	if err := unmarshal(&val); err != nil {
		return err
	}

	if err := lsn.Parse(val); err != nil {
		return fmt.Errorf("could not parse lsn %q: %v", val, err)
	}

	*l = lsn

	return nil
}

func (l *LSN) Bytes() []byte {
	return []byte(l.String())
}
