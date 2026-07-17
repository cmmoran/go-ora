package go_ora

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cmmoran/go-ora/v2/advanced_nego"
	"github.com/cmmoran/go-ora/v2/configurations"
	"github.com/cmmoran/go-ora/v2/converters"
)

type testNTSManager struct{}

func (*testNTSManager) NewNegotiateMessage(string, string) ([]byte, error) { return nil, nil }
func (*testNTSManager) ProcessChallenge([]byte, string, string) ([]byte, error) {
	return nil, nil
}

func TestParseConfigKeepsNTSAuthenticationSessionLocal(t *testing.T) {
	legacy := &testNTSManager{}
	SetNTSAuth(legacy)
	t.Cleanup(func() { SetNTSAuth(&advanced_nego.NTSAuthDefault{}) })

	config, err := ParseConfig("oracle://user:pass@database.example:1521/service?OS+PASSWORD=hash")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := config.NTS.(*advanced_nego.NTSAuthHash); !ok {
		t.Fatalf("OS password did not select session-specific hash authentication: %T", config.NTS)
	}
	if advanced_nego.NTSAuth != legacy {
		t.Fatal("parsing a DSN changed the process-global NTS authenticator")
	}
}

type failingNetConn struct {
	mu     sync.Mutex
	closed bool
}

func (conn *failingNetConn) Read([]byte) (int, error)         { return 0, errors.New("read failed") }
func (conn *failingNetConn) Write([]byte) (int, error)        { return 0, errors.New("write failed") }
func (conn *failingNetConn) LocalAddr() net.Addr              { return nil }
func (conn *failingNetConn) RemoteAddr() net.Addr             { return nil }
func (conn *failingNetConn) SetDeadline(time.Time) error      { return nil }
func (conn *failingNetConn) SetReadDeadline(time.Time) error  { return nil }
func (conn *failingNetConn) SetWriteDeadline(time.Time) error { return nil }
func (conn *failingNetConn) Close() error {
	conn.mu.Lock()
	conn.closed = true
	conn.mu.Unlock()
	return nil
}

func (conn *failingNetConn) isClosed() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	return conn.closed
}

func TestValidatorInterface(t *testing.T) {
	var conn interface{} = &Connection{}
	if _, ok := conn.(driver.Validator); !ok {
		t.Fatal("Connection does not implement driver.Validator")
	}
}

func TestSessionResetterInterface(t *testing.T) {
	var conn interface{} = &Connection{}
	if _, ok := conn.(driver.SessionResetter); !ok {
		t.Fatal("Connection does not implement driver.SessionResetter")
	}
}

func TestConnectionIsValid(t *testing.T) {
	conn := &Connection{State: Closed}
	if conn.IsValid() {
		t.Fatal("closed connection should be invalid")
	}

	conn.State = Opened
	if !conn.IsValid() {
		t.Fatal("opened non-bad connection should be valid")
	}

	conn.bad.Store(true)
	if conn.IsValid() {
		t.Fatal("bad connection should be invalid")
	}
}

func TestConnectionBadStateConcurrentAccess(t *testing.T) {
	conn := &Connection{State: Opened}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			conn.setBad()
		}()
		go func() {
			defer wg.Done()
			_ = conn.IsValid()
			_ = conn.ResetSession(context.Background())
		}()
	}
	wg.Wait()
	if conn.IsValid() {
		t.Fatal("bad connection should be invalid")
	}
}

func TestOpenWithContextCleansUpPartialConnection(t *testing.T) {
	netConn := &failingNetConn{}
	config := &configurations.ConnectionConfig{
		DatabaseInfo: configurations.DatabaseInfo{
			Servers:     []configurations.ServerAddr{{Addr: "database.example", Port: 1521}},
			ServiceName: "service",
		},
		SessionInfo: configurations.SessionInfo{
			Protocol:              "tcp",
			SessionDataUnitSize:   0xFFFF,
			TransportDataUnitSize: 0xFFFF,
		},
	}
	config.RegisterDial(func(context.Context, string, string) (net.Conn, error) {
		return netConn, nil
	})
	conn, err := NewConnection("", config)
	if err != nil {
		t.Fatal(err)
	}

	if err = conn.OpenWithContext(context.Background()); err == nil {
		t.Fatal("expected open failure")
	}
	if !netConn.isClosed() {
		t.Fatal("partially opened network connection was not closed")
	}
	if conn.session != nil {
		t.Fatal("failed open retained its network session")
	}
	if conn.State != Closed {
		t.Fatalf("failed open left connection state %v", conn.State)
	}
}

func TestQueryContextRejectsInvalidWrappedResultSet(t *testing.T) {
	conn := &Connection{}
	if _, err := conn.QueryContext(context.Background(), wrapResultset, nil); err == nil {
		t.Fatal("expected missing wrapped result set error")
	}
	args := []driver.NamedValue{{Value: "not rows"}}
	if _, err := conn.QueryContext(context.Background(), wrapResultset, args); err == nil {
		t.Fatal("expected invalid wrapped result set error")
	}
}

func TestResetSessionMethod(t *testing.T) {
	conn := &Connection{State: Opened}
	if err := conn.ResetSession(context.Background()); err != nil {
		t.Fatalf("expected nil error for healthy connection, got %v", err)
	}

	conn.bad.Store(true)
	if err := conn.ResetSession(context.Background()); err != driver.ErrBadConn {
		t.Fatalf("expected driver.ErrBadConn, got %v", err)
	}
}

func TestGetStrConvCreatesConverterWhenConnectionCacheIsEmpty(t *testing.T) {
	conn := &Connection{}
	conv, err := conn.getStrConv(0x230)
	if err != nil {
		t.Fatalf("expected converter, got error: %v", err)
	}
	if conv == nil {
		t.Fatal("expected non-nil converter")
	}
	if conv.GetLangID() != 0x230 {
		t.Fatalf("expected charset 0x230, got %#x", conv.GetLangID())
	}
}

func TestGetStrConvPrefersClientConverterWhenCharsetMatches(t *testing.T) {
	clientConv := converters.NewStringConverter(0x230)
	serverConv := converters.NewStringConverter(0x230)
	conn := &Connection{cStrConv: clientConv, sStrConv: serverConv}

	conv, err := conn.getStrConv(0x230)
	if err != nil {
		t.Fatalf("expected converter, got error: %v", err)
	}
	if conv != clientConv {
		t.Fatal("expected client converter to be preferred")
	}
}

func TestDriverDoesNotCloneNegotiatedStringConverters(t *testing.T) {
	drv := NewDriver()
	drv.sStrConv = converters.NewStringConverter(0x230)
	drv.nStrConv = converters.NewStringConverter(0x7D0)

	serverClone, nationalClone := drv.cloneStringConverters()
	if serverClone != nil || nationalClone != nil {
		t.Fatalf("negotiated converters leaked into a new connection: %v, %v", serverClone, nationalClone)
	}
}

type testDriverProvider struct {
	driver driver.Driver
}

func (provider testDriverProvider) Driver() driver.Driver {
	return provider.driver
}

func TestDriverClonesExplicitStringConverters(t *testing.T) {
	drv := NewDriver()
	serverConv := converters.NewStringConverter(0x230)
	nationalConv := converters.NewStringConverter(0x7D0)
	SetStringConverter(testDriverProvider{driver: drv}, serverConv, nationalConv)

	serverClone, nationalClone := drv.cloneStringConverters()
	if serverClone == nil || serverClone.GetLangID() != serverConv.GetLangID() {
		t.Fatalf("unexpected server converter clone: %v", serverClone)
	}
	if nationalClone == nil || nationalClone.GetLangID() != nationalConv.GetLangID() {
		t.Fatalf("unexpected national converter clone: %v", nationalClone)
	}
	if serverClone == serverConv || nationalClone == nationalConv {
		t.Fatal("connection converters must not share mutable converter instances with the driver")
	}
}

func TestDriverStringConverterConfigurationConcurrent(t *testing.T) {
	drv := NewDriver()
	provider := testDriverProvider{driver: drv}
	serverConv := converters.NewStringConverter(0x230)
	nationalConv := converters.NewStringConverter(0x7D0)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for index := 0; index < 100; index++ {
			SetStringConverter(provider, serverConv, nationalConv)
			SetStringConverter(provider, nil, nil)
		}
	}()
	go func() {
		defer wg.Done()
		for index := 0; index < 100; index++ {
			_, _ = drv.cloneStringConverters()
		}
	}()
	wg.Wait()
}

func TestCustomTypeRegistryConcurrentSnapshots(t *testing.T) {
	registry := newCustomTypeRegistry()
	const count = 100
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for index := 0; index < count; index++ {
			registry.set(fmt.Sprintf("TYPE_%d", index), customType{name: fmt.Sprintf("TYPE_%d", index)})
		}
	}()
	go func() {
		defer wg.Done()
		for index := 0; index < count; index++ {
			for name, value := range registry.snapshot() {
				if name != value.name {
					t.Errorf("registry snapshot mismatch: %q != %q", name, value.name)
				}
			}
		}
	}()
	wg.Wait()
	if got := len(registry.snapshot()); got != count {
		t.Fatalf("expected %d registered types, got %d", count, got)
	}
}

func TestBuildBulkInsertArgs(t *testing.T) {
	columns := make([][]driver.Value, 101)
	for index := range columns {
		columns[index] = []driver.Value{index, index + 1, index + 2}
	}

	args, err := buildBulkInsertArgs(2, columns)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != len(columns) {
		t.Fatalf("expected %d column arguments, got %d", len(columns), len(args))
	}
	for index, arg := range args {
		value, ok := arg.(*batch)
		if !ok {
			t.Fatalf("column %d: expected *batch, got %T", index, arg)
		}
		rows, ok := value.array.([]driver.Value)
		if !ok || len(rows) != 2 {
			t.Fatalf("column %d: expected two rows, got %#v", index, value.array)
		}
	}
}

func TestBuildBulkInsertArgsRejectsInvalidShape(t *testing.T) {
	tests := []struct {
		name    string
		rowNum  int
		columns [][]driver.Value
	}{
		{name: "negative rows", rowNum: -1},
		{name: "missing columns", rowNum: 1},
		{name: "short column", rowNum: 2, columns: [][]driver.Value{{1}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := buildBulkInsertArgs(test.rowNum, test.columns); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
