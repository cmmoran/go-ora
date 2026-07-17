package network

import (
	"crypto/tls"
	"net"
	"testing"

	"github.com/cmmoran/go-ora/v2/configurations"
	"github.com/cmmoran/go-ora/v2/trace"
)

func TestNegotiateDoesNotMutateSharedTLSConfig(t *testing.T) {
	shared := &tls.Config{}
	for _, host := range []string{"database-a.example", "database-b.example"} {
		client, server := net.Pipe()
		config := &configurations.ConnectionConfig{
			DatabaseInfo: configurations.DatabaseInfo{
				Servers: []configurations.ServerAddr{{Addr: host}},
			},
			SessionInfo: configurations.SessionInfo{TLSConfig: shared},
		}
		session := NewSession(config, trace.NilTracer())
		session.conn = client
		session.negotiate()
		_ = client.Close()
		_ = server.Close()
	}

	if shared.ServerName != "" {
		t.Fatalf("shared TLS config was mutated with server name %q", shared.ServerName)
	}
}

func TestNegotiatePreservesExplicitTLSServerName(t *testing.T) {
	shared := &tls.Config{ServerName: "certificate-name.example"}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	config := &configurations.ConnectionConfig{
		DatabaseInfo: configurations.DatabaseInfo{
			Servers: []configurations.ServerAddr{{Addr: "database.example"}},
		},
		SessionInfo: configurations.SessionInfo{TLSConfig: shared},
	}
	session := NewSession(config, trace.NilTracer())
	session.conn = client
	session.negotiate()

	if shared.ServerName != "certificate-name.example" {
		t.Fatalf("explicit server name was overwritten with %q", shared.ServerName)
	}
}
