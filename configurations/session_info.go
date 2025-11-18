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

func NewDNSAwareDialer(timeout time.Duration, servers ...string) *net.Dialer {
	r := newCustomResolver(servers, timeout)

	return &net.Dialer{
		Timeout:  timeout,
		Resolver: r,
	}
}

type serverHealth struct {
	badTill time.Time
}

type customResolver struct {
	servers []string
	health  []serverHealth
	timeout time.Duration
}

func newCustomResolver(servers []string, timeout time.Duration) *net.Resolver {
	cr := &customResolver{
		servers: servers,
		timeout: timeout,
		health:  make([]serverHealth, len(servers)),
	}

	return &net.Resolver{
		PreferGo: true,
		Dial:     cr.dial,
	}
}

func (c *customResolver) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	d := net.Dialer{Timeout: c.timeout}

	// try each healthy server udp → tcp
	for i, s := range c.servers {
		if time.Now().Before(c.health[i].badTill) {
			continue
		}

		if conn, err := d.DialContext(ctx, "udp", s); err == nil {
			return conn, nil
		} else {
			c.health[i].badTill = time.Now().Add(30 * time.Second)
		}

		if conn, err := d.DialContext(ctx, "tcp", s); err == nil {
			return conn, nil
		} else {
			c.health[i].badTill = time.Now().Add(30 * time.Second)
		}
	}

	// fallback: try everything even if marked bad
	for _, s := range c.servers {
		if conn, err := d.DialContext(ctx, "udp", s); err == nil {
			return conn, nil
		}
		if conn, err := d.DialContext(ctx, "tcp", s); err == nil {
			return conn, nil
		}
	}

	return nil, fmt.Errorf("resolver: no servers reachable")
}
