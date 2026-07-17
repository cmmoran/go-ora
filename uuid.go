package go_ora

import (
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/cmmoran/go-ora/v2/converters"
)

// ErrInvalidUUID reports that a value is not a supported UUID representation.
var ErrInvalidUUID = errors.New("go_ora: invalid UUID")

// UUIDString stores a UUID in canonical dashed form. It binds to Oracle as
// RAW(16) and scans either RAW(16) or a supported UUID string representation.
type UUIDString string

// ParseUUIDString validates value and returns its canonical dashed form.
func ParseUUIDString(value string) (UUIDString, error) {
	raw, ok := converters.EncodeUUIDLike(value)
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrInvalidUUID, value)
	}
	return formatUUIDString(raw), nil
}

// String returns the canonical dashed UUID representation.
func (value UUIDString) String() string {
	return string(value)
}

// Value implements driver.Valuer and returns the UUID's 16-byte RAW value.
func (value UUIDString) Value() (driver.Value, error) {
	raw, err := value.Bytes()
	if err != nil {
		return nil, err
	}
	output := make([]byte, len(raw))
	copy(output, raw[:])
	return output, nil
}

// Scan implements sql.Scanner.
func (value *UUIDString) Scan(src interface{}) error {
	if value == nil {
		return errors.New("go_ora: cannot scan UUID into nil pointer")
	}
	if src == nil {
		*value = ""
		return nil
	}

	var raw []byte
	switch input := src.(type) {
	case string:
		parsed, ok := converters.EncodeUUIDLike(input)
		if !ok {
			return fmt.Errorf("%w: %q", ErrInvalidUUID, input)
		}
		raw = parsed
	case []byte:
		if len(input) == 16 {
			raw = input
		} else {
			parsed, ok := converters.EncodeUUIDLike(string(input))
			if !ok {
				return fmt.Errorf("%w: %q", ErrInvalidUUID, input)
			}
			raw = parsed
		}
	default:
		return fmt.Errorf("go_ora: cannot scan UUID from %T", src)
	}

	*value = formatUUIDString(raw)
	return nil
}

// Bytes returns the UUID as 16 bytes.
func (value UUIDString) Bytes() ([16]byte, error) {
	var output [16]byte
	raw, ok := converters.EncodeUUIDLike(string(value))
	if !ok {
		return output, fmt.Errorf("%w: %q", ErrInvalidUUID, value)
	}
	copy(output[:], raw)
	return output, nil
}

// Hex returns the compact 32-character hexadecimal representation used by
// Oracle RAWTOHEX.
func (value UUIDString) Hex() (string, error) {
	raw, err := value.Bytes()
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func formatUUIDString(raw []byte) UUIDString {
	var output [36]byte
	hex.Encode(output[0:8], raw[0:4])
	output[8] = '-'
	hex.Encode(output[9:13], raw[4:6])
	output[13] = '-'
	hex.Encode(output[14:18], raw[6:8])
	output[18] = '-'
	hex.Encode(output[19:23], raw[8:10])
	output[23] = '-'
	hex.Encode(output[24:36], raw[10:16])
	return UUIDString(output[:])
}
