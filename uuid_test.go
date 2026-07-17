package go_ora

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

const testUUIDCanonical = "a40b65f9-5d1d-415c-a2ac-fea0933c8d4e"
const testUUIDCompact = "a40b65f95d1d415ca2acfea0933c8d4e"

var testUUIDRaw = []byte{0xa4, 0x0b, 0x65, 0xf9, 0x5d, 0x1d, 0x41, 0x5c, 0xa2, 0xac, 0xfe, 0xa0, 0x93, 0x3c, 0x8d, 0x4e}

func TestUUIDStringInterfaces(t *testing.T) {
	var _ driver.Valuer = UUIDString("")
	var _ sql.Scanner = (*UUIDString)(nil)
}

func TestParseUUIDStringCanonicalizesSupportedFormats(t *testing.T) {
	for _, input := range []string{testUUIDCanonical, testUUIDCompact, "A40B65F9-5D1D-415C-A2AC-FEA0933C8D4E"} {
		value, err := ParseUUIDString(input)
		if err != nil {
			t.Fatalf("parse %q: %v", input, err)
		}
		if value.String() != testUUIDCanonical {
			t.Fatalf("expected %q, got %q", testUUIDCanonical, value)
		}
	}
}

func TestUUIDStringValueReturnsRawBytes(t *testing.T) {
	value, err := UUIDString(testUUIDCanonical).Value()
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := value.([]byte)
	if !ok || !bytes.Equal(raw, testUUIDRaw) {
		t.Fatalf("expected RAW UUID %x, got %T %x", testUUIDRaw, value, raw)
	}
}

func TestUUIDStringScanAndRepresentations(t *testing.T) {
	inputs := []interface{}{testUUIDCanonical, testUUIDCompact, append([]byte(nil), testUUIDRaw...), []byte(testUUIDCanonical)}
	for _, input := range inputs {
		var value UUIDString
		if err := value.Scan(input); err != nil {
			t.Fatalf("scan %T: %v", input, err)
		}
		if value.String() != testUUIDCanonical {
			t.Fatalf("expected %q, got %q", testUUIDCanonical, value)
		}
		hexValue, err := value.Hex()
		if err != nil {
			t.Fatal(err)
		}
		if hexValue != testUUIDCompact {
			t.Fatalf("expected %q, got %q", testUUIDCompact, hexValue)
		}
		raw, err := value.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw[:], testUUIDRaw) {
			t.Fatalf("expected %x, got %x", testUUIDRaw, raw)
		}
	}
}

func TestUUIDStringRejectsInvalidValues(t *testing.T) {
	for _, input := range []string{"", "a40b-65f95d1d415ca2acfea0933c8d4e", "not-a-uuid"} {
		if _, err := ParseUUIDString(input); !errors.Is(err, ErrInvalidUUID) {
			t.Fatalf("expected ErrInvalidUUID for %q, got %v", input, err)
		}
		if _, err := UUIDString(input).Value(); !errors.Is(err, ErrInvalidUUID) {
			t.Fatalf("expected Value to reject %q, got %v", input, err)
		}
	}

	for _, input := range []interface{}{[]byte{}, []byte("invalid"), 42} {
		var value UUIDString
		if err := value.Scan(input); err == nil {
			t.Fatalf("expected Scan to reject %T(%v)", input, input)
		}
	}

	var nilValue *UUIDString
	if err := nilValue.Scan(testUUIDCanonical); err == nil {
		t.Fatal("expected Scan on a nil UUIDString pointer to fail")
	}
}

func TestUUIDStringScanNullClearsValue(t *testing.T) {
	value := UUIDString(testUUIDCanonical)
	if err := value.Scan(nil); err != nil {
		t.Fatal(err)
	}
	if value != "" {
		t.Fatalf("expected empty UUID after NULL scan, got %q", value)
	}
}
