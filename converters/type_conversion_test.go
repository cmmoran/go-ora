package converters

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

func TestEncodeUUIDLike(t *testing.T) {
	want := []byte{0xa4, 0x0b, 0x65, 0xf9, 0x5d, 0x1d, 0x41, 0x5c, 0xa2, 0xac, 0xfe, 0xa0, 0x93, 0x3c, 0x8d, 0x4e}
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{name: "canonical", value: "a40b65f9-5d1d-415c-a2ac-fea0933c8d4e", valid: true},
		{name: "compact", value: "a40b65f95d1d415ca2acfea0933c8d4e", valid: true},
		{name: "uppercase", value: "A40B65F9-5D1D-415C-A2AC-FEA0933C8D4E", valid: true},
		{name: "misplaced hyphens", value: "a40b-65f95d1d415ca2acfea0933c8d4e", valid: false},
		{name: "extra hyphens", value: "-a40b65f95d1d415ca2acfea0933c8d4e-", valid: false},
		{name: "invalid hex", value: "g40b65f95d1d415ca2acfea0933c8d4e", valid: false},
		{name: "braced", value: "{a40b65f9-5d1d-415c-a2ac-fea0933c8d4e}", valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, valid := EncodeUUIDLike(test.value)
			if valid != test.valid {
				t.Fatalf("expected valid=%v, got %v (%x)", test.valid, valid, got)
			}
			if valid && !bytes.Equal(got, want) {
				t.Fatalf("expected %x, got %x", want, got)
			}
		})
	}
}

// Some documentation:
//	https://gotodba.com/2015/03/24/how-are-numbers-saved-in-oracle/

func TestDecodeDouble(t *testing.T) {
	for _, tt := range TestFloatValue {
		t.Run(tt.SelectText, func(t *testing.T) {
			got := DecodeDouble(tt.Binary)
			e := math.Abs((got - tt.Float) / tt.Float)
			if e > 1e-15 {
				t.Errorf("DecodeDouble() = %v, want %v, Error= %e", got, tt.Float, e)
			}
		})
	}
}

func TestDecodeInt(t *testing.T) {
	for _, tt := range TestFloatValue {
		// Test only with interger values
		if tt.IsInteger {
			t.Run(tt.SelectText, func(t *testing.T) {
				got := DecodeInt(tt.Binary)
				if got != tt.Integer {
					t.Errorf("DecodeInt() = %v, want %v", got, tt.Integer)
				}
			})
		}
	}
}

func TestTypeOfDecodeNumber(t *testing.T) {
	for _, tt := range TestFloatValue {
		t.Run(tt.SelectText, func(t *testing.T) {
			got := DecodeNumber(tt.Binary)

			if i, ok := got.(int64); ok {
				if !tt.IsInteger {
					t.Errorf("Expecting a float64(%g), got an int64(%d)", tt.Float, i)
					return
				}
				if i != tt.Integer {
					t.Errorf("Expecting an int64(%d), got %d", tt.Integer, i)
				}
			} else if f, ok := got.(float64); ok {
				if tt.IsInteger {
					t.Errorf("Expecting a int64(%d), got a float(%g)", tt.Integer, f)
					return
				}
				e := math.Abs((f - tt.Float) / tt.Float)
				if e > 1e-15 {
					t.Errorf("Expecting an float64(%g), got %g", tt.Float, f)
				}
			}
		})
	}
}

func TestEncodeInt64(t *testing.T) {
	for _, tt := range TestFloatValue {
		// Test only with interger values
		if tt.IsInteger {
			t.Run(tt.SelectText, func(t *testing.T) {
				got := EncodeInt64(tt.Integer)

				n2 := DecodeInt(got)
				if n2 != tt.Integer {
					t.Errorf("DecodeInt(EncodeInt64(%d)) = %v", tt.Integer, n2)
				}

				if !reflect.DeepEqual(got, tt.Binary) {
					t.Errorf("EncodeInt64() = %v, want %v", got, tt.Binary)
				}
			})
		}
	}
}

func TestEncodeUint64(t *testing.T) {
	var x uint64 = 0xFFFFFFFFFFFFFFFE
	intVal := EncodeInt64(int64(x))
	uintVal := EncodeUint64(x)
	t.Logf("Enode int64: %#v", intVal)
	t.Logf("Encode uint64: %#v", uintVal)
}

func TestEncodeInt(t *testing.T) {
	for _, tt := range TestFloatValue {
		// Test only with interger values
		if tt.IsInteger && tt.Float >= math.MinInt64 && tt.Float <= math.MaxInt64 {
			t.Run(tt.SelectText, func(t *testing.T) {
				i := int(tt.Integer)
				got := EncodeInt(i)

				n2 := int(DecodeInt(got))
				if n2 != i {
					t.Errorf("DecodeInt(EncodeInt(%d)) = %v", i, n2)
				}

				if !reflect.DeepEqual(got, tt.Binary) {
					t.Errorf("EncodeInt() = %v, want %v", got, tt.Binary)
				}
			})
		}
	}
}

func TestEncodeDouble(t *testing.T) {
	for _, tt := range TestFloatValue {
		t.Run(tt.SelectText, func(t *testing.T) {
			got, err := EncodeDouble(tt.Float)
			if err != nil {
				t.Errorf("Unexpected error: %s", err)
				return
			}

			f := DecodeDouble(got)

			if tt.Float != 0.0 {
				e := math.Abs((f - tt.Float) / tt.Float)
				if e > 1e-15 {
					t.Errorf("DecodeDouble(EncodeDouble(%g)) = %g,  Error= %e", tt.Float, f, e)
				}
			}

			if len(tt.Binary) < 10 {
				if !reflect.DeepEqual(tt.Binary, got) {
					t.Errorf("EncodeDouble(%g) = %v want %v", tt.Float, got, tt.Binary)
				}
			}
		})
	}
}

//
// func TestEncodeDate(t *testing.T) {
// 	ti := time.Date(2006, 01, 02, 15, 04, 06, 0, time.UTC)

// 	got := EncodeDate(ti)
// 	want := []byte{214, 7, 1, 2, 15, 4, 5, 0}

// 	if !reflect.DeepEqual(got, want) {
// 		t.Errorf("EncodeDate(%v) = %v, want %v", ti, got, want)
// 	}
// }

func TestDecodeDate(t *testing.T) {
	// 57 3c
	// 0001
	// 0101
	// data := []byte{0x78, 0x7b, 0xb, 0xa, 0x16, 0x31, 0x12, 0x30, 0xb4, 0xa0, 0xc0, 0x17, 0x3c}
	data := []byte{0x78, 0x7C, 0x01, 0x08, 0x11, 0x23, 0x32, 0x28, 0xE9, 0x6E, 0x70, 0x84, 0x80}
	date, err := DecodeDate(data)
	if err != nil {
		t.Error(err)
	}
	// result: 2024-01-08 19:34:49.6863868 +0300 +03
	t.Log(date)
}
