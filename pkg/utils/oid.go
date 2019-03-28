package utils

import (
	"database/sql/driver"
	"fmt"

	"github.com/jackc/pgx/pgtype"
)

// OID describes pg oid
type OID uint32

// String implements Stringer
func (o OID) String() string {
	return fmt.Sprintf("%d", uint32(o))
}

// Scan implements the Scanner interface in order to allow pgx to read OID values from the DB.
func (o *OID) Scan(src interface{}) error {
	var result pgtype.OID
	if err := result.Scan(src); err != nil {
		return err
	}
	*o = OID(result)
	return nil
}

// Value implements sql Valuer
func (o OID) Value() (driver.Value, error) {
	return int64(o), nil
}
