package network

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"

	"github.com/cmmoran/go-ora/v2/configurations"
)

func TestConnectRejectsExcessiveProtocolTransitions(t *testing.T) {
	session := NewSession(&configurations.ConnectionConfig{}, nil)
	if err := session.connect(context.Background(), maxConnectRedirects, maxConnectRedirects); !errors.Is(err, ErrConnectTransitionLimit) {
		t.Fatalf("expected connect transition limit, got %v", err)
	}
}

func TestReadPacketRejectsExcessiveResends(t *testing.T) {
	session := NewSessionWithInputBufferForDebug(nil)
	if _, err := session.readPacketWithResendCount(maxResendRequests); !errors.Is(err, ErrResendLimit) {
		t.Fatalf("expected resend limit, got %v", err)
	}
}

func TestAcceptPacketRejectsTruncatedModernHeader(t *testing.T) {
	packet := make([]byte, 32)
	binary.BigEndian.PutUint16(packet[8:], 315)
	if got := newAcceptPacketFromData(packet, &configurations.ConnectionConfig{}); got != nil {
		t.Fatal("expected truncated modern accept packet to be rejected")
	}
}

func TestAcceptPacketRejectsOutOfRangeDataOffset(t *testing.T) {
	packet := make([]byte, 40)
	binary.BigEndian.PutUint16(packet[8:], 315)
	binary.BigEndian.PutUint16(packet[20:], 41)
	if got := newAcceptPacketFromData(packet, &configurations.ConnectionConfig{}); got != nil {
		t.Fatal("expected invalid accept data offset to be rejected")
	}
}

func TestReadPacketRejectsInvalidRedirectDataLength(t *testing.T) {
	packet := make([]byte, 11)
	binary.BigEndian.PutUint16(packet, uint16(len(packet)))
	packet[4] = byte(REDIRECT)
	binary.BigEndian.PutUint16(packet[8:], 2)

	session := NewSessionWithInputBufferForDebug(nil)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	session.conn = client
	session.reader = bufio.NewReader(client)
	go func() {
		_, _ = server.Write(packet)
	}()
	if _, err := session.readPacket(); err == nil {
		t.Fatal("expected invalid redirect data length error")
	}
}
