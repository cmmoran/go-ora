package go_ora

import (
	"reflect"
	"testing"
)

func TestIsRawByteValue(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		value any
		want  bool
	}{
		{value: []byte{1}, want: true},
		{value: [16]byte{}, want: true},
		{value: [15]byte{}, want: false},
		{value: []string{"x"}, want: false},
		{value: "x", want: false},
	} {
		if got := isRawByteValue(reflect.TypeOf(test.value)); got != test.want {
			t.Fatalf("isRawByteValue(%T) = %t, want %t", test.value, got, test.want)
		}
	}
}
