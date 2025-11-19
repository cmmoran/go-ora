package configurations

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"
)

type DialerContext interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type SessionInfo struct {
	SSLVersion            string
	ConnectTimeout        time.Duration
	Timeout               time.Duration
	EnableOOB             bool
	UnixAddress           string
	TransportDataUnitSize uint32
	SessionDataUnitSize   uint32
	Protocol              string
	SSL                   bool
	SSLVerify             bool
	TLSConfig             *tls.Config
	Dialer                DialerContext
}

func (si *SessionInfo) RegisterDial(dialer func(ctx context.Context, network, address string) (net.Conn, error)) {
	if dialer != nil {
		var temp = &customDial{DialCtx: dialer}
		si.Dialer = temp
	} else {
		si.Dialer = nil
	}
}

type customDial struct {
	DialCtx func(ctx context.Context, network, address string) (net.Conn, error)
}

func (c *customDial) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return c.DialCtx(ctx, network, address)
}

func (si *SessionInfo) UpdateSSL(server *ServerAddr) error {
	if server != nil {
		if strings.ToLower(server.Protocol) == "tcps" {
			si.SSL = true
			return nil
		} else if strings.ToLower(server.Protocol) == "tcp" {
			si.SSL = false
			return nil
		}
	}
	if strings.ToLower(si.Protocol) == "tcp" {
		si.SSL = false
	} else if strings.ToLower(si.Protocol) == "tcps" {
		si.SSL = true
	} else {
		return fmt.Errorf("unknown or missing protocol: %s", si.Protocol)
	}
	return nil
}

type dnsServer struct {
	Network string // "udp" or "tcp"
	Address string // host:port
}

func NewDNSAwareDialer(timeout time.Duration, servers ...string) DialerContext {
	parsed := make([]dnsServer, 0, len(servers))

	for _, raw := range servers {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}

		netw := "udp"

		switch {
		case strings.HasSuffix(s, "/udp"):
			netw = "udp"
			s = strings.TrimSuffix(s, "/udp")
		case strings.HasSuffix(s, "/tcp"):
			netw = "tcp"
			s = strings.TrimSuffix(s, "/tcp")
		}

		if _, _, err := net.SplitHostPort(s); err != nil {
			s = net.JoinHostPort(s, "53")
		}

		parsed = append(parsed, dnsServer{
			Network: netw,
			Address: s,
		})
	}

	if len(parsed) == 0 {
		// Fallback to system behavior if nothing is configured
		return &net.Dialer{Timeout: timeout}
	}

	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _network, _address string) (net.Conn, error) {
			d := &net.Dialer{Timeout: timeout}

			var lastErr error
			for _, srv := range parsed {
				conn, err := d.DialContext(ctx, srv.Network, srv.Address)
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			if lastErr == nil {
				lastErr = fmt.Errorf("no DNS servers configured")
			}
			return nil, lastErr
		},
	}

	d := &net.Dialer{Resolver: resolver, Timeout: timeout}
	return &customDial{
		DialCtx: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return d.DialContext(ctx, network, addr)
		},
	}
}
