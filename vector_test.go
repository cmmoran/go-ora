package go_ora

import (
	"reflect"
	"testing"
)

func TestNewVectorPointerSlicesPreserveElements(t *testing.T) {
	bytesInput := []*uint8{ptrTo(uint8(10)), ptrTo(uint8(20)), ptrTo(uint8(30))}
	float32Input := []*float32{ptrTo(float32(-10.5)), ptrTo(float32(20.25))}
	float64Input := []*float64{ptrTo(float64(1.25)), ptrTo(float64(-2.5))}

	tests := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{name: "uint8", input: bytesInput, want: []byte{10, 20, 30}},
		{name: "float32", input: float32Input, want: []float32{-10.5, 20.25}},
		{name: "float64", input: float64Input, want: []float64{1.25, -2.5}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vector, err := NewVector(test.input)
			if err != nil {
				t.Fatal(err)
			}
			if vector.Count != reflect.ValueOf(test.want).Len() {
				t.Fatalf("expected count %d, got %d", reflect.ValueOf(test.want).Len(), vector.Count)
			}
			if !reflect.DeepEqual(vector.Data, test.want) {
				t.Fatalf("expected data %#v, got %#v", test.want, vector.Data)
			}
		})
	}
}

func ptrTo[T any](value T) *T {
	return &value
}
