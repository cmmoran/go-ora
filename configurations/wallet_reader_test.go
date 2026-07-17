package configurations

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestNewWalletFromReaderRejectsMalformedData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty"},
		{name: "magic only", data: []byte{161, 248, 78}},
		{name: "incomplete fixed header", data: walletTestHeader(54, 17, nil)[:12]},
		{name: "invalid magic", data: append([]byte{0, 0, 0}, walletTestHeader(54, 0, []byte{5})[3:]...)},
		{name: "truncated AES key", data: walletTestHeader(54, 17, []byte{6})},
		{name: "truncated AES password", data: walletTestHeader(54, 33, append([]byte{6}, make([]byte, 16)...))},
		{name: "truncated DES password", data: walletTestHeader(54, 0, []byte{0x35})},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewWalletFromReader(bytes.NewReader(test.data)); err == nil {
				t.Fatal("expected malformed wallet data to fail")
			}
		})
	}

	if _, err := NewWalletFromReader(nil); err == nil {
		t.Fatal("expected nil reader to fail")
	}
	var nilReader *bytes.Reader
	if _, err := NewWalletFromReader(nilReader); err == nil {
		t.Fatal("expected typed nil reader to fail")
	}
}

func TestNewWalletFromReaderWrapsReadError(t *testing.T) {
	want := errors.New("reader failed")
	if _, err := NewWalletFromReader(errorReader{err: want}); !errors.Is(err, want) {
		t.Fatalf("expected wrapped reader error, got %v", err)
	}
}

func walletTestHeader(version byte, size uint32, trailing []byte) []byte {
	var data bytes.Buffer
	data.Write([]byte{161, 248, 78, version})
	_ = binary.Write(&data, binary.BigEndian, uint32(6))
	_ = binary.Write(&data, binary.BigEndian, size)
	data.Write(trailing)
	return data.Bytes()
}

type errorReader struct {
	err error
}

func (reader errorReader) Read([]byte) (int, error) {
	return 0, reader.err
}
