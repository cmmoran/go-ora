package network

import (
	"encoding/binary"
	"testing"
)

func TestPacketBodyLengthRejectsInvalidLengths(t *testing.T) {
	session := NewSessionWithInputBufferForDebug(nil)
	session.Context.TransportDataUnit = 32

	tests := []struct {
		name   string
		length uint16
	}{
		{name: "shorter than header", length: 7},
		{name: "larger than transport unit", length: 33},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			head := make([]byte, 8)
			binary.BigEndian.PutUint16(head, test.length)
			if _, err := session.packetBodyLength(head); err == nil {
				t.Fatal("expected invalid packet length error")
			}
		})
	}
}

func TestPacketBodyLengthReturnsPayloadSize(t *testing.T) {
	session := NewSessionWithInputBufferForDebug(nil)
	session.Context.TransportDataUnit = 32
	head := make([]byte, 8)
	binary.BigEndian.PutUint16(head, 20)

	length, err := session.packetBodyLength(head)
	if err != nil {
		t.Fatal(err)
	}
	if length != 12 {
		t.Fatalf("expected 12-byte body, got %d", length)
	}
}

func TestGetStringRejectsShortCLR(t *testing.T) {
	session := NewSessionWithInputBufferForDebug([]byte{3, 'a', 'b', 'c'})
	if _, err := session.GetString(4); err == nil {
		t.Fatal("expected short decoded string error")
	}
}
