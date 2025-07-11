package go_ora

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sijms/go-ora/v2/lazy_init"

	"github.com/sijms/go-ora/v2/configurations"
	"github.com/sijms/go-ora/v2/trace"

	"github.com/sijms/go-ora/v2/advanced_nego"
	"github.com/sijms/go-ora/v2/converters"
	"github.com/sijms/go-ora/v2/network"
)

type ConnectionState int

const (
	Closed ConnectionState = 0
	Opened ConnectionState = 1
)

type LogonMode int

const (
	NoNewPass   LogonMode = 0x1
	SysDba      LogonMode = 0x20
	SysOper     LogonMode = 0x40
	SysAsm      LogonMode = 0x00400000
	SysBackup   LogonMode = 0x01000000
	SysDg       LogonMode = 0x02000000
	SysKm       LogonMode = 0x04000000
	SysRac      LogonMode = 0x08000000
	UserAndPass LogonMode = 0x100
	// WithNewPass LogonMode = 0x2
	// PROXY       LogonMode = 0x400
)

// from GODROR
const wrapResultset = "--WRAP_RESULTSET--"

// Querier is the QueryContext of sql.Conn.
type Querier interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

// ///

type NLSData struct {
	Calender        string `db:"p_nls_calendar,,40,out"`
	Comp            string `db:"p_nls_comp,,40,out"`
	Language        string
	LengthSemantics string `db:"p_nls_length_semantics,,40,out"`
	NCharConvExcep  string `db:"p_nls_nchar_conv_excep,,40,out"`
	NCharConvImp    string
	DateLang        string `db:"p_nls_date_lang,,40,out"`
	Sort            string `db:"p_nls_sort,,40,out"`
	Currency        string `db:"p_nls_currency,,40,out"`
	DateFormat      string `db:"p_nls_date_format,,40,out"`
	TimeFormat      string
	IsoCurrency     string `db:"p_nls_iso_currency,,40,out"`
	NumericChars    string `db:"p_nls_numeric_chars,,40,out"`
	DualCurrency    string `db:"p_nls_dual_currency,,40,out"`
	UnionCurrency   string
	Timestamp       string `db:"p_nls_timestamp,,48,out"`
	TimestampTZ     string `db:"p_nls_timestamp_tz,,56,out"`
	TTimezoneFormat string
	NTimezoneFormat string
	Territory       string
	Charset         string
}
type Connection struct {
	State             ConnectionState
	LogonMode         LogonMode
	autoCommit        bool
	tracer            trace.Tracer
	connOption        *configurations.ConnectionConfig
	session           *network.Session
	tcpNego           *TCPNego
	dataNego          *DataTypeNego
	authObject        *AuthObject
	SessionProperties map[string]string
	dBVersion         *DBVersion
	sessionID         int
	serialID          int
	transactionID     []byte
	sStrConv          converters.IStringConverter
	nStrConv          converters.IStringConverter
	cStrConv          converters.IStringConverter
	NLSData           NLSData
	cusTyp            map[string]customType
	maxLen            struct {
		varchar   int
		nvarchar  int
		raw       int
		number    int
		date      int
		timestamp int
	}
	bad                      bool
	dbTimeZone               *time.Location // equivalent to database timezone used for timestamp with local timezone
	dbServerTimeZone         *time.Location // equivalent to timezone of the server carry the database
	dbServerTimeZoneExplicit *time.Location
}

type OracleConnector struct {
	drv           *OracleDriver
	connectString string
	dialer        configurations.DialerContext
	tlsConfig     *tls.Config
	kerberos      configurations.KerberosAuthInterface
}

func NewConnector(connString string) driver.Connector {
	return &OracleConnector{connectString: connString, drv: NewDriver()}
}

func (driver *OracleDriver) OpenConnector(connString string) (driver.Connector, error) {
	// create hash from connection string
	return &OracleConnector{drv: driver, connectString: connString}, nil
}

func (connector *OracleConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := NewConnection(connector.connectString, connector.drv.connOption)
	if err != nil {
		return nil, err
	}
	conn.cusTyp = connector.drv.cusTyp
	if connector.drv.sStrConv != nil {
		conn.sStrConv = connector.drv.sStrConv.Clone()
	}
	if connector.drv.nStrConv != nil {
		conn.nStrConv = connector.drv.nStrConv.Clone()
	}
	if conn.connOption.Dialer == nil {
		conn.connOption.Dialer = connector.dialer
	}
	if conn.connOption.TLSConfig == nil {
		conn.connOption.TLSConfig = connector.tlsConfig
	}
	if conn.connOption.Kerberos == nil {
		conn.connOption.Kerberos = connector.kerberos
	}
	err = conn.OpenWithContext(ctx)
	if err != nil {
		return nil, err
	}
	err = connector.drv.init(conn)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (connector *OracleConnector) Driver() driver.Driver {
	return connector.drv
}

func (connector *OracleConnector) Dialer(dialer configurations.DialerContext) {
	connector.dialer = dialer
}

func (connector *OracleConnector) WithTLSConfig(config *tls.Config) {
	connector.tlsConfig = config
}

// WithKerberosAuth sets the Kerberos authenticator to be used by this connector. It does not enable the Kerberos; set AUTH TYPE to KERBEROS to do so.
func (connector *OracleConnector) WithKerberosAuth(auth configurations.KerberosAuthInterface) {
	connector.kerberos = auth
}

// Open return a new open connection
func (driver *OracleDriver) Open(name string) (driver.Conn, error) {
	conn, err := NewConnection(name, driver.connOption)
	if err != nil {
		return nil, err
	}
	conn.cusTyp = driver.cusTyp
	err = conn.Open()
	if err != nil {
		return nil, err
	}
	err = driver.init(conn)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// GetNLS return NLS properties of the connection.
// this function is left from v1. but v2 is using another method
func (conn *Connection) GetNLS() (*NLSData, error) {
	// we read from sys.nls_session_parameters ONCE
	cmdText := `
	BEGIN
		SELECT 
			MAX(CASE WHEN PARAMETER='NLS_CALENDAR' THEN VALUE END) AS NLS_CALENDAR,
			MAX(CASE WHEN PARAMETER='NLS_COMP' THEN VALUE END) AS NLS_COMP,
			MAX(CASE WHEN PARAMETER='NLS_LENGTH_SEMANTICS' THEN VALUE END) AS NLS_LENGTH_SEMANTICS,
			MAX(CASE WHEN PARAMETER='NLS_NCHAR_CONV_EXCP' THEN VALUE END) AS NLS_NCHAR_CONV_EXCP,
			MAX(CASE WHEN PARAMETER='NLS_DATE_LANGUAGE' THEN VALUE END) AS NLS_DATE_LANGUAGE,
			MAX(CASE WHEN PARAMETER='NLS_SORT' THEN VALUE END) AS NLS_SORT,
			MAX(CASE WHEN PARAMETER='NLS_CURRENCY' THEN VALUE END) AS NLS_CURRENCY,
			MAX(CASE WHEN PARAMETER='NLS_DATE_FORMAT' THEN VALUE END) AS NLS_DATE_FORMAT,
			MAX(CASE WHEN PARAMETER='NLS_ISO_CURRENCY' THEN VALUE END) AS NLS_ISO_CURRENCY,
			MAX(CASE WHEN PARAMETER='NLS_NUMERIC_CHARACTERS' THEN VALUE END) AS NLS_NUMERIC_CHARACTERS,
			MAX(CASE WHEN PARAMETER='NLS_DUAL_CURRENCY' THEN VALUE END) AS NLS_DUAL_CURRENCY,
			MAX(CASE WHEN PARAMETER='NLS_TIMESTAMP_FORMAT' THEN VALUE END) AS NLS_TIMESTAMP_FORMAT,
			MAX(CASE WHEN PARAMETER='NLS_TIMESTAMP_TZ_FORMAT' THEN VALUE END) AS NLS_TIMESTAMP_TZ_FORMAT
			into :p_nls_calendar, :p_nls_comp, :p_nls_length_semantics, :p_nls_nchar_conv_excep, 
				:p_nls_date_lang, :p_nls_sort, :p_nls_currency, :p_nls_date_format, :p_nls_iso_currency,
				:p_nls_numeric_chars, :p_nls_dual_currency, :p_nls_timestamp, :p_nls_timestamp_tz
		FROM
			sys.nls_session_parameters
		;
	END;`
	stmt := NewStmt(cmdText, conn)
	defer func(stmt *Stmt) {
		_ = stmt.Close()
	}(stmt)
	_, err := stmt.Exec([]driver.Value{&conn.NLSData})
	if err != nil {
		return nil, err
	}

	// fmt.Println(stmt.Pars)

	// if len(stmt.Pars) >= 10 {
	//	conn.NLSData.Calender = conn.sStrConv.Decode(stmt.Pars[0].BValue)
	//	conn.NLSData.Comp = conn.sStrConv.Decode(stmt.Pars[1].BValue)
	//	conn.NLSData.LengthSemantics = conn.sStrConv.Decode(stmt.Pars[2].BValue)
	//	conn.NLSData.NCharConvExcep = conn.sStrConv.Decode(stmt.Pars[3].BValue)
	//	conn.NLSData.DateLang = conn.sStrConv.Decode(stmt.Pars[4].BValue)
	//	conn.NLSData.Sort = conn.sStrConv.Decode(stmt.Pars[5].BValue)
	//	conn.NLSData.Currency = conn.sStrConv.Decode(stmt.Pars[6].BValue)
	//	conn.NLSData.DateFormat = conn.sStrConv.Decode(stmt.Pars[7].BValue)
	//	conn.NLSData.IsoCurrency = conn.sStrConv.Decode(stmt.Pars[8].BValue)
	//	conn.NLSData.NumericChars = conn.sStrConv.Decode(stmt.Pars[9].BValue)
	//	conn.NLSData.DualCurrency = conn.sStrConv.Decode(stmt.Pars[10].BValue)
	//	conn.NLSData.Timestamp = conn.sStrConv.Decode(stmt.Pars[11].BValue)
	//	conn.NLSData.TimestampTZ = conn.sStrConv.Decode(stmt.Pars[12].BValue)
	// }

	/*
		for _, par := range stmt.Pars {
			if par.Name == "p_nls_calendar" {
				conn.NLSData.Calender = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_comp" {
				conn.NLSData.Comp = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_length_semantics" {
				conn.NLSData.LengthSemantics = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_nchar_conv_excep" {
				conn.NLSData.NCharConvExcep = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_date_lang" {
				conn.NLSData.DateLang = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_sort" {
				conn.NLSData.Sort = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_currency" {
				conn.NLSData.Currency = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_date_format" {
				conn.NLSData.DateFormat = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_iso_currency" {
				conn.NLSData.IsoCurrency = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_numeric_chars" {
				conn.NLSData.NumericChars = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_dual_currency" {
				conn.NLSData.DualCurrency = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_timestamp" {
				conn.NLSData.Timestamp = conn.strConv.Decode(par.BValue)
			} else if par.Name == "p_nls_timestamp_tz" {
				conn.NLSData.TimestampTZ = conn.strConv.Decode(par.BValue)
			}
		}
	*/

	return &conn.NLSData, nil
}

// Prepare take a query string and create a stmt object
func (conn *Connection) Prepare(query string) (driver.Stmt, error) {
	conn.tracer.Print("Prepare\n", query)
	return NewStmt(query, conn), nil
}

// Ping test if connection is online
func (conn *Connection) Ping(ctx context.Context) error {
	conn.tracer.Print("Ping")
	conn.session.ResetBuffer()
	done := conn.session.StartContext(ctx)
	defer conn.session.EndContext(done)
	return (&simpleObject{
		connection:  conn,
		operationID: 0x93,
		data:        nil,
	}).exec()
}

func (conn *Connection) getDefaultCharsetID() int {
	if conn.cStrConv != nil {
		return conn.cStrConv.GetLangID()
	}
	return conn.tcpNego.ServerCharset
}
func (conn *Connection) getDefaultStrConv() (converters.IStringConverter, error) {
	return conn.getStrConv(conn.getDefaultCharsetID())
}
func (conn *Connection) getStrConv(charsetID int) (converters.IStringConverter, error) {
	if conn.sStrConv != nil && charsetID == conn.sStrConv.GetLangID() {
		if conn.cStrConv != nil {
			return conn.cStrConv, nil
		}
		return conn.sStrConv, nil
	}

	if conn.nStrConv != nil && charsetID == conn.nStrConv.GetLangID() {
		return conn.nStrConv, nil
	}
	return conn.sStrConv, nil
	//temp := converters.NewStringConverter(charsetID)
	//if temp == nil {
	//	return nil, fmt.Errorf("server requested charset id: %d which is not supported by the driver", charsetID)
	//}
	//return temp, nil
}

func (conn *Connection) Logoff() error {
	conn.tracer.Print("Logoff")
	session := conn.session
	session.ResetBuffer()
	// session.PutBytes(0x11, 0x87, 0, 0, 0, 0x2, 0x1, 0x11, 0x1, 0, 0, 0, 0x1, 0, 0, 0, 0, 0, 0x1, 0, 0, 0, 0, 0,
	//	3, 9, 0)
	session.PutBytes(3, 9, 0)
	err := session.Write()
	if err != nil {
		return err
	}
	return conn.read()
	// loop := true
	// for loop {
	//	msg, err := session.GetByte()
	//	if err != nil {
	//		return err
	//	}
	//	err = conn.readMsg(msg)
	//	if err != nil {
	//		return err
	//	}
	//	if msg == 4 || msg == 9 {
	//		loop = false
	//	}
	// }
	// return nil
}

func (conn *Connection) read() error {
	loop := true
	session := conn.session
	for loop {
		msg, err := session.GetByte()
		if err != nil {
			return err
		}
		err = conn.readMsg(msg)
		if err != nil {
			return err
		}
		if msg == 4 || msg == 9 {
			loop = false
		}
	}
	return nil
}

// Open the connection = bring it online
func (conn *Connection) Open() error {
	return conn.OpenWithContext(context.Background())
}

// func (conn *Connection) restore() error {
//	tracer := conn.tracer
//	failOver := conn.connOption.Failover
//	var err error
//	for trial := 0; trial < failOver; trial++ {
//		tracer.Print("reconnect trial #", trial+1)
//		err = conn.Open()
//		if err != nil {
//			tracer.Print("Error: ", err)
//			continue
//		}
//		break
//	}
//	return err
// }

// OpenWithContext open the connection with timeout context
func (conn *Connection) OpenWithContext(ctx context.Context) error {
	if len(conn.connOption.TraceDir) > 0 {
		if err := os.MkdirAll(conn.connOption.TraceDir, os.ModePerm); err == nil {
			now := time.Now()
			traceFileName := fmt.Sprintf("%s/trace_%d_%02d_%02d_%02d_%02d_%02d_%d.log", conn.connOption.TraceDir,
				now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(),
				now.Nanosecond())
			if tr, err := os.Create(traceFileName); err == nil {
				conn.tracer = trace.NewTraceWriter(tr)
			}
		}
	} else {
		if len(conn.connOption.TraceFilePath) > 0 {
			tf, err := os.Create(conn.connOption.TraceFilePath)
			if err != nil {
				//noinspection GoErrorStringFormat
				return fmt.Errorf("Can't open trace file: %w", err)
			}
			conn.tracer = trace.NewTraceWriter(tf)
		} else {
			conn.tracer = trace.NilTracer()
		}
	}
	tracer := conn.tracer
	switch conn.connOption.DBAPrivilege {
	case configurations.SYSDBA:
		conn.LogonMode |= SysDba
	case configurations.SYSOPER:
		conn.LogonMode |= SysOper
	case configurations.SYSASM:
		conn.LogonMode |= SysAsm
	case configurations.SYSBACKUP:
		conn.LogonMode |= SysBackup
	case configurations.SYSDG:
		conn.LogonMode |= SysDg
	case configurations.SYSKM:
		conn.LogonMode |= SysKm
	case configurations.SYSRAC:
		conn.LogonMode |= SysRac
	default:
		conn.LogonMode = 0
	}
	conn.connOption.ResetServerIndex()
	conn.session = network.NewSession(conn.connOption, conn.tracer)
	W := conn.connOption.Wallet
	if conn.connOption.SSL && W != nil {
		err := conn.session.LoadSSLData(W.Certificates, W.PrivateKeys, W.CertificateRequests)
		if err != nil {
			return err
		}
	}
	session := conn.session
	// start check for context
	done := session.StartContext(ctx)
	defer session.EndContext(done)
	err := session.Connect(ctx)
	if err != nil {
		return err
	}
	// advanced negotiation
	if session.Context.ACFL0&1 != 0 && session.Context.ACFL0&4 == 0 && session.Context.ACFL1&8 == 0 {
		tracer.Print("Advance Negotiation")
		ano, err := advanced_nego.NewAdvNego(session, conn.tracer, conn.connOption)
		if err != nil {
			return err
		}
		err = ano.Write()
		if err != nil {
			return err
		}
		err = ano.Read()
		if err != nil {
			return err
		}
		err = ano.StartServices()
		if err != nil {
			return err
		}
	}

	err = conn.protocolNegotiation()
	if err != nil {
		return err
	}
	err = conn.dataTypeNegotiation()
	if err != nil {
		return err
	}
	err = conn.doAuth()
	if errors.Is(err, network.ErrConnReset) {
		err = conn.read()
	}
	if err != nil {
		return err
	}
	conn.State = Opened
	conn.dBVersion, err = GetDBVersion(conn.session)
	if err != nil {
		return err
	}
	conn.session.SetConnected()
	tracer.Print("Connected")
	tracer.Print("Database Version: ", conn.dBVersion.Text)
	sessionID, err := strconv.ParseUint(conn.SessionProperties["AUTH_SESSION_ID"], 10, 32)
	if err != nil {
		return err
	}
	conn.sessionID = int(sessionID)
	serialNum, err := strconv.ParseUint(conn.SessionProperties["AUTH_SERIAL_NUM"], 10, 32)
	if err != nil {
		return err
	}
	conn.serialID = int(serialNum)
	conn.connOption.InstanceName = conn.SessionProperties["AUTH_SC_INSTANCE_NAME"]
	// conn.connOption.Host = conn.SessionProperties["AUTH_SC_SERVER_HOST"]
	conn.connOption.ServiceName = conn.SessionProperties["AUTH_SC_SERVICE_NAME"]
	conn.connOption.DomainName = conn.SessionProperties["AUTH_SC_DB_DOMAIN"]
	conn.connOption.DBName = conn.SessionProperties["AUTH_SC_DBUNIQUE_NAME"]
	if len(conn.NLSData.Language) == 0 {
		_, err = conn.GetNLS()
		if err != nil {
			tracer.Print("Error getting NLS: ", err)
		}
	}
	conn.getDBServerTimeZone()
	return nil
}

func (conn *Connection) getDBServerTimeZone() {

	if conn.connOption.DatabaseInfo.Location != "" {
		loc, err := time.LoadLocation(conn.connOption.DatabaseInfo.Location)
		if err == nil {
			conn.dbServerTimeZone = loc
			return
		}
		conn.tracer.Printf("Unable to configure timezone from LOCATION parameter: %v", err)
	}

	if conn.dbServerTimeZoneExplicit != nil {
		conn.dbServerTimeZone = conn.dbServerTimeZoneExplicit
		return
	}

	var current time.Time
	err := conn.QueryRowContext(context.Background(), "SELECT SYSTIMESTAMP FROM DUAL", nil).Scan(&current)
	if err != nil {
		conn.dbServerTimeZone = time.UTC
	}
	conn.dbServerTimeZone = current.Location()
}

func (conn *Connection) getDBTimeZone() error {
	var result string
	err := conn.QueryRowContext(context.Background(), "SELECT DBTIMEZONE FROM DUAL", nil).Scan(&result)
	// var current time.Time
	// err := conn.QueryRowContext(context.Background(), "SELECT SYSTIMESTAMP FROM DUAL", nil).Scan(&current)
	if err != nil {
		return err
	}
	var tzHours, tzMin int
	_, err = fmt.Sscanf(result, "%03d:%02d", &tzHours, &tzMin)
	if err != nil {
		return err
	}
	conn.dbTimeZone = time.FixedZone(result, tzHours*60*60+tzMin*60)

	return nil
}

// Begin a transaction
func (conn *Connection) Begin() (driver.Tx, error) {
	conn.tracer.Print("Begin transaction")
	conn.autoCommit = false
	return &Transaction{conn: conn, ctx: context.Background()}, nil
}

func (conn *Connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, errors.New("readonly transaction is not supported")
	}
	if opts.Isolation != 0 {
		return nil, errors.New("only support default value for isolation")
	}
	conn.tracer.Print("Begin transaction with context")
	conn.autoCommit = false
	return &Transaction{conn: conn, ctx: ctx}, nil
}

// NewConnection create a new connection from databaseURL string or configuration
func NewConnection(databaseUrl string, config *configurations.ConnectionConfig) (*Connection, error) {
	var err error
	if len(databaseUrl) > 0 {
		config, err = ParseConfig(databaseUrl)
		if err != nil {
			return nil, err
		}
	} else {
		if config == nil {
			return nil, errors.New("database url or configuration is required")
		}
	}

	// conStr, err := newConnectionStringFromUrl(databaseUrl)
	temp := new(configurations.ConnectionConfig)
	*temp = *config
	return &Connection{
		State:      Closed,
		connOption: temp,
		cStrConv:   converters.NewStringConverter(config.CharsetID),
		autoCommit: true,
		cusTyp:     map[string]customType{},
		maxLen: struct {
			varchar   int
			nvarchar  int
			raw       int
			number    int
			date      int
			timestamp int
		}{varchar: 0x7FFF, nvarchar: 0x7FFF, raw: 0x7FFF, number: 0x16, date: 0xB, timestamp: 0xB},
	}, nil
}

// Close the connection by disconnect network session
func (conn *Connection) Close() (err error) {
	tracer := conn.tracer
	tracer.Print("Close")
	// var err error = nil
	if conn.session != nil {
		err = conn.Logoff()
		if err != nil {
			tracer.Print("Logoff with error: ", err)
		}
		err = conn.session.WriteFinalPacket()
		if err != nil {
			tracer.Print("Write Final Packet With Error: ", err)
		}
		conn.session.Disconnect()
		conn.session = nil
	}
	conn.State = Closed
	conn.tracer.Print("Connection Closed")
	_ = conn.tracer.Close()
	return
}

// doAuth a login step that occur during open connection
func (conn *Connection) doAuth() error {
	conn.tracer.Print("doAuth")
	if len(conn.connOption.UserID) > 0 && len(conn.connOption.Password) > 0 {
		conn.session.ResetBuffer()
		conn.session.PutBytes(3, 0x76, 0, 1)
		conn.session.PutUint(len(conn.connOption.UserID), 4, true, true)
		conn.LogonMode = conn.LogonMode | NoNewPass
		conn.session.PutUint(int(conn.LogonMode), 4, true, true)
		conn.session.PutBytes(1, 1, 5, 1, 1)
		if len(conn.connOption.UserID) > 0 {
			conn.session.PutString(conn.connOption.UserID)
		}
		conn.session.PutKeyValString("AUTH_TERMINAL", conn.connOption.ClientInfo.HostName, 0)
		conn.session.PutKeyValString("AUTH_PROGRAM_NM", conn.connOption.ClientInfo.ProgramName, 0)
		conn.session.PutKeyValString("AUTH_MACHINE", conn.connOption.ClientInfo.HostName, 0)
		conn.session.PutKeyValString("AUTH_PID", fmt.Sprintf("%d", conn.connOption.ClientInfo.PID), 0)
		conn.session.PutKeyValString("AUTH_SID", conn.connOption.ClientInfo.OSUserName, 0)
		err := conn.session.Write()
		if err != nil {
			return err
		}

		conn.authObject, err = newAuthObject(conn.connOption.UserID, conn.connOption.Password, conn.tcpNego, conn)
		if err != nil {
			return err
		}
	} else {
		conn.authObject = &AuthObject{
			tcpNego:    conn.tcpNego,
			usePadding: false,
			customHash: conn.tcpNego.ServerCompileTimeCaps[4]&32 != 0,
		}
	}
	// if proxyAuth ==> mode |= PROXY
	err := conn.authObject.Write(conn.connOption, conn.LogonMode, conn.session)
	if err != nil {
		return err
	}
	stop := false
	for !stop {
		msg, err := conn.session.GetByte()
		if err != nil {
			return err
		}
		switch msg {
		case 8:
			dictLen, err := conn.session.GetInt(2, true, true)
			if err != nil {
				return err
			}
			conn.SessionProperties = make(map[string]string, dictLen)
			for x := 0; x < dictLen; x++ {
				key, val, _, err := conn.session.GetKeyVal()
				if err != nil {
					return err
				}
				conn.SessionProperties[string(key)] = string(val)
			}
		// case 27:
		//	this.ProcessImplicitResultSet(ref implicitRSList);
		//	continue;
		default:
			err = conn.readMsg(msg)
			if err != nil {
				return err
			}
			if msg == 4 || msg == 9 {
				stop = true
			}
			// return errors.New(fmt.Sprintf("message code error: received code %d", msg))
		}
	}

	// if verifyResponse == true
	// conn.authObject.VerifyResponse(conn.SessionProperties["AUTH_SVR_RESPONSE"])
	return nil
}

// loadNLSData get nls data for v2
func (conn *Connection) loadNLSData() error {
	_, err := conn.session.GetInt(2, true, true)
	if err != nil {
		return err
	}
	_, err = conn.session.GetByte()
	if err != nil {
		return err
	}
	length, err := conn.session.GetInt(4, true, true)
	if err != nil {
		return err
	}
	_, err = conn.session.GetByte()
	if err != nil {
		return err
	}
	for i := 0; i < length; i++ {
		nlsKey, nlsVal, nlsCode, err := conn.session.GetKeyVal()
		if err != nil {
			return err
		}
		conn.NLSData.SaveNLSValue(string(nlsKey), string(nlsVal), nlsCode)
	}
	_, err = conn.session.GetInt(4, true, true)
	return err
}

func (conn *Connection) getServerNetworkInformation(code uint8) error {
	session := conn.session
	if code == 0 {
		_, err := session.GetByte()
		return err
	}
	switch code - 1 {
	case 1:
		// receive OCOSPID
		length, err := session.GetInt(2, true, true)
		if err != nil {
			return err
		}
		_, err = session.GetByte()
		if err != nil {
			return err
		}
		_, err = session.GetBytes(length)
		if err != nil {
			return err
		}
	case 3:
		// receive OCSESSRET session return values
		_, err := session.GetInt(2, true, true)
		if err != nil {
			return err
		}
		_, err = session.GetByte()
		if err != nil {
			return err
		}
		length, err := session.GetInt(2, true, true)
		if err != nil {
			return err
		}
		// get nls data
		for i := 0; i < length; i++ {
			nlsKey, nlsVal, nlsCode, err := session.GetKeyVal()
			if err != nil {
				return err
			}
			conn.NLSData.SaveNLSValue(string(nlsKey), string(nlsVal), nlsCode)
		}
		flag, err := session.GetInt(4, true, true)
		if err != nil {
			return err
		}
		sessionID, err := session.GetInt(4, true, true)
		if err != nil {
			return err
		}
		serialID, err := session.GetInt(2, true, true)
		if err != nil {
			return err
		}
		if flag&4 == 4 {
			conn.sessionID = sessionID
			conn.serialID = serialID
			// save session id and serial number to connection
		}
	case 4:
		err := conn.loadNLSData()
		if err != nil {
			return err
		}
	case 6:
		length, err := session.GetInt(4, true, true)
		if err != nil {
			return err
		}
		conn.transactionID, err = session.GetClr()
		if len(conn.transactionID) > length {
			conn.transactionID = conn.transactionID[:length]
		}
	case 7:
		_, err := session.GetInt(2, true, true)
		if err != nil {
			return err
		}
		_, err = session.GetByte()
		if err != nil {
			return err
		}
		_, err = session.GetInt(4, true, true)
		if err != nil {
			return err
		}
		_, err = session.GetInt(4, true, true)
		if err != nil {
			return err
		}
		_, err = session.GetByte()
		if err != nil {
			return err
		}
		_, err = session.GetDlc()
		if err != nil {
			return err
		}
	case 8:
		_, err := session.GetInt(2, true, true)
		if err != nil {
			return err
		}
		_, err = session.GetByte()
		if err != nil {
			return err
		}
	}
	return nil
}

// SaveNLSValue a helper function that convert between nls key and code
func (nls *NLSData) SaveNLSValue(key, value string, code int) {
	key = strings.ToUpper(key)
	if len(key) > 0 {
		switch key {
		case "AUTH_NLS_LXCCURRENCY":
			code = 0
		case "AUTH_NLS_LXCISOCURR":
			code = 1
		case "AUTH_NLS_LXCNUMERICS":
			code = 2
		case "AUTH_NLS_LXCDATEFM":
			code = 7
		case "AUTH_NLS_LXCDATELANG":
			code = 8
		case "AUTH_NLS_LXCTERRITORY":
			code = 9
		case "SESSION_NLS_LXCCHARSET":
			code = 10
		case "AUTH_NLS_LXCSORT":
			code = 11
		case "AUTH_NLS_LXCCALENDAR":
			code = 12
		case "AUTH_NLS_LXLAN":
			code = 16
		case "AL8KW_NLSCOMP":
			code = 50
		case "AUTH_NLS_LXCUNIONCUR":
			code = 52
		case "AUTH_NLS_LXCTIMEFM":
			code = 57
		case "AUTH_NLS_LXCSTMPFM":
			code = 58
		case "AUTH_NLS_LXCTTZNFM":
			code = 59
		case "AUTH_NLS_LXCSTZNFM":
			code = 60
		case "SESSION_NLS_LXCNLSLENSEM":
			code = 61
		case "SESSION_NLS_LXCNCHAREXCP":
			code = 62
		case "SESSION_NLS_LXCNCHARIMP":
			code = 63
		}
	}
	switch code {
	case 0:
		nls.Currency = value
	case 1:
		nls.IsoCurrency = value
	case 2:
		nls.NumericChars = value
	case 7:
		nls.DateFormat = value
	case 8:
		nls.DateLang = value
	case 9:
		nls.Territory = value
	case 10:
		nls.Charset = value
	case 11:
		nls.Sort = value
	case 12:
		nls.Calender = value
	case 16:
		nls.Language = value
	case 50:
		nls.Comp = value
	case 52:
		nls.UnionCurrency = value
	case 57:
		nls.TimeFormat = value
	case 58:
		nls.Timestamp = value
	case 59:
		nls.TTimezoneFormat = value
	case 60:
		nls.NTimezoneFormat = value
	case 61:
		nls.LengthSemantics = value
	case 62:
		nls.NCharConvExcep = value
	case 63:
		nls.NCharConvImp = value
	}
}

func (conn *Connection) Exec(text string, args ...driver.Value) (driver.Result, error) {
	stmt := NewStmt(text, conn)
	defer func() {
		_ = stmt.Close()
	}()
	return stmt.Exec(args)
}

func SetNTSAuth(newNTSManager advanced_nego.NTSAuthInterface) {
	advanced_nego.NTSAuth = newNTSManager
}

var insertQueryBracketsRegexp = lazy_init.NewLazyInit(func() (interface{}, error) {
	return regexp.Compile(`\((.*?)\)`)
})

// StructsInsert support interface{} array
//func (conn *Connection) StructsInsert(sqlText string, values []interface{}) (driver.Result, error) {
//	return conn.BulkInsert(sqlText, len(values), values...)
//	//if len(values) == 0 {
//	//	return nil, nil
//	//}
//	//
//	//var insertQueryBracketsRegexpAny interface{}
//	//insertQueryBracketsRegexpAny, err = insertQueryBracketsRegexp.GetValue()
//	//if err != nil {
//	//	return nil, err
//	//}
//	//
//	//matchArray := insertQueryBracketsRegexpAny.(*regexp.Regexp).FindStringSubmatch(sqlText)
//	//if len(matchArray) < 2 {
//	//	return nil, fmt.Errorf("invalid sql must be like: insert into a (name,age) values (:1,:2)")
//	//}
//	//fields := strings.Split(matchArray[1], ",")
//	//fieldsMap := make(map[string]int)
//	//for i, field := range fields {
//	//	fieldsMap[strings.TrimSpace(strings.ToLower(field))] = i
//	//}
//	//_type := reflect.TypeOf(values[0])
//	//result := make([][]driver.Value, _type.NumField())
//	//idx := 0
//	//for i := 0; i < _type.NumField(); i++ {
//	//	db := _type.Field(i).Tag.Get("db")
//	//	db = strings.TrimSpace(strings.ToLower(db))
//	//	if db != "" {
//	//		if _, ok := fieldsMap[db]; ok {
//	//			f := make([]driver.Value, len(values))
//	//			result[idx] = f
//	//			idx++
//	//		}
//	//	}
//	//}
//	//if idx != len(fieldsMap) {
//	//	return nil, &network.OracleError{ErrCode: 947, ErrMsg: "ORA-00947: Not enough values"}
//	//}
//	//for i, item := range values {
//	//	_value := reflect.ValueOf(item)
//	//	for j := 0; j < _type.NumField(); j++ {
//	//		db := _type.Field(j).Tag.Get("db")
//	//		if db != "" {
//	//			if v, ok := fieldsMap[strings.ToLower(db)]; ok {
//	//				if !_value.Field(j).IsValid() {
//	//					result[v][i] = nil
//	//				} else {
//	//					result[v][i] = _value.Field(j).Interface()
//	//				}
//	//			}
//	//		}
//	//
//	//	}
//	//}
//	//return conn.BulkInsert(sqlText, len(values), result...)
//}

// BulkInsert mass insert column values into a table
// all columns should pass as an array of values
func (conn *Connection) BulkInsert(sqlText string, rowNum int, columns ...[]driver.Value) (driver.Result, error) {
	stmt := NewStmt(sqlText, conn)
	stmt.autoClose = true
	input := make([]driver.Value, 100)
	for x := 0; x < rowNum; x++ {
		input[x] = NewBatch(columns[x])
	}
	result, err := stmt.Exec(input)
	if err != nil {
		_ = stmt.Close()
		return nil, err
	}
	err = stmt.Close()
	return result, err
	//if conn.State != Opened {
	//	return nil, &network.OracleError{ErrCode: 6413, ErrMsg: "ORA-06413: Connection not open"}
	//}
	//if rowNum == 0 {
	//	return nil, nil
	//}
	//stmt := NewStmt(sqlText, conn)
	//stmt.arrayBindCount = rowNum
	//defer func() {
	//	_ = stmt.Close()
	//}()
	//tracer := conn.tracer
	//tracer.Printf("BulkInsert:\n%s", stmt.text)
	//tracer.Printf("Row Num: %d", rowNum)
	//tracer.Printf("Column Num: %d", len(columns))
	//for idx, col := range columns {
	//	if len(col) < rowNum {
	//		return nil, fmt.Errorf("size of column no. %d is less than rowNum", idx)
	//	}
	//
	//	par, err := stmt.NewParam("", col[0], 0, Input)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	maxLen := par.MaxLen
	//	maxCharLen := par.MaxCharLen
	//	dataType := par.DataType
	//
	//	for index, val := range col {
	//		if index == 0 {
	//			continue
	//		}
	//		par.Value = val
	//		err = par.encodeValue(0, conn)
	//		if err != nil {
	//			return nil, err
	//		}
	//
	//		if maxLen < par.MaxLen {
	//			maxLen = par.MaxLen
	//		}
	//
	//		if maxCharLen < par.MaxCharLen {
	//			maxCharLen = par.MaxCharLen
	//		}
	//
	//		if par.DataType != dataType && par.DataType != NCHAR {
	//			dataType = par.DataType
	//		}
	//	}
	//	par.Value = col[0]
	//	_ = par.encodeValue(0, conn)
	//	par.MaxLen = maxLen
	//	par.MaxCharLen = maxCharLen
	//	par.DataType = dataType
	//	stmt.Pars = append(stmt.Pars, *par)
	//}
	//session := conn.session
	//session.ResetBuffer()
	//err := stmt.basicWrite(stmt.getExeOption(), stmt.parse, stmt.define)
	//for x := 0; x < rowNum; x++ {
	//	for idx, col := range columns {
	//		stmt.Pars[idx].Value = col[x]
	//		err = stmt.Pars[idx].encodeValue(0, conn)
	//		if err != nil {
	//			return nil, err
	//		}
	//	}
	//	err = stmt.writePars()
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	//err = session.Write()
	//if err != nil {
	//	return nil, err
	//}
	//dataSet := new(DataSet)
	//err = stmt.read(dataSet)
	//if err != nil {
	//	return nil, err
	//}
	//result := new(QueryResult)
	//if session.Summary != nil {
	//	result.rowsAffected = int64(session.Summary.CurRowNumber)
	//}
	//return result, nil
}

func (conn *Connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt := NewStmt(query, conn)
	stmt.autoClose = true
	result, err := stmt.ExecContext(ctx, args)
	if err != nil {
		_ = stmt.Close()
		return nil, err
	}
	err = stmt.Close()
	return result, err
}

func (conn *Connection) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(driver.Valuer); ok {
		return driver.ErrSkip
	}

	return nil
}

func (conn *Connection) QueryRowContext(ctx context.Context, query string, args []driver.NamedValue) *DataSet {
	stmt := NewStmt(query, conn)
	stmt.autoClose = true
	rows, err := stmt.QueryContext(ctx, args)
	if err != nil {
		dataSet := &DataSet{}
		dataSet.currentResultSet().lastErr = err
		return dataSet
	}
	dataSet, ok := rows.(*DataSet)
	if !ok {
		dataSet.currentResultSet().lastErr = fmt.Errorf("expected DataSet, got %T", rows)
		return dataSet
	}
	dataSet.Next_()
	return dataSet
}

func WrapRefCursor(ctx context.Context, q Querier, cursor *RefCursor) (*sql.Rows, error) {
	rows, err := cursor.Query()
	if err != nil {
		return nil, err
	}
	return q.QueryContext(ctx, wrapResultset, rows)
}

func (conn *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query == wrapResultset {
		return args[0].Value.(driver.Rows), nil
	}
	stmt := NewStmt(query, conn)
	stmt.autoClose = true
	rows, err := stmt.QueryContext(ctx, args)
	if err != nil {
		_ = stmt.Close()
	}
	return rows, err
}

func (conn *Connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if conn.State != Opened {
		return nil, driver.ErrBadConn
	}
	conn.tracer.Print("Prepare With Context\n", query)
	done := conn.session.StartContext(ctx)
	defer conn.session.EndContext(done)
	return NewStmt(query, conn), nil
}

func (conn *Connection) readMsg(msgCode uint8) error {
	session := conn.session
	tracer := conn.tracer
	var err error
	switch msgCode {
	case 4:
		// if conn.session.IsBreak() {
		//	if conn.session.RestoreIndex() {
		//		_, _ = conn.session.GetByte()
		//	}
		//	conn.session.ResetBreak()
		// }
		conn.session.Summary, err = network.NewSummary(session)
		if err != nil {
			return err
		}
		tracer.Printf("Summary: RetCode:%d, Error Message:%q", session.Summary.RetCode, string(session.Summary.ErrorMessage))
		if conn.session.HasError() {
			return conn.session.GetError()
		}
	case 8:
		size, err := session.GetInt(2, true, true)
		if err != nil {
			return err
		}
		for x := 0; x < size; x++ {
			_, err = session.GetInt(4, true, true)
			if err != nil {
				return err
			}
		}
		// for x := 0; x < 2; x++ {
		//	_, err = session.GetInt(4, true, true)
		//	if err != nil {
		//		return err
		//	}
		// }
		// for x := 2; x < size; x++ {
		//	_, err = session.GetInt(4, true, true)
		//	if err != nil {
		//		return err
		//	}
		// }
		_, err = session.GetInt(2, true, true)
		if err != nil {
			return err
		}
		size, err = session.GetInt(2, true, true)
		for x := 0; x < size; x++ {
			_, val, num, err := session.GetKeyVal()
			if err != nil {
				return err
			}
			// fmt.Println(key, val, num)
			if num == 163 {
				session.TimeZone = val
				// fmt.Println("session time zone", session.TimeZone)
			}
		}
		if session.TTCVersion >= 4 {
			// get queryID
			size, err = session.GetInt(4, true, true)
			if err != nil {
				return err
			}
			if size > 0 {
				bty, err := session.GetBytes(size)
				if err != nil {
					return err
				}
				if len(bty) >= 8 {
					queryID := binary.LittleEndian.Uint64(bty[size-8:])
					os.Stderr.WriteString(fmt.Sprintln("query ID: ", queryID))
				}
			}
		}
		if session.TTCVersion >= 7 {
			length, err := session.GetInt(4, true, true)
			if err != nil {
				return err
			}
			for i := 0; i < length; i++ {
				_, err = session.GetInt(8, true, true)
				if err != nil {
					return err
				}
			}
		}
	case 9:
		if session.HasEOSCapability {
			temp, err := session.GetInt(4, true, true)
			if err != nil {
				return err
			}
			if session.Summary != nil {
				session.Summary.EndOfCallStatus = temp
			}
		}
		if session.HasFSAPCapability {
			if session.Summary == nil {
				session.Summary = new(network.SummaryObject)
			}
			session.Summary.EndToEndECIDSequence, err = session.GetInt(2, true, true)
			if err != nil {
				return err
			}
		}
	case 15:
		warning, err := network.NewWarningObject(session)
		if err != nil {
			return err
		}
		if warning != nil {
			os.Stderr.WriteString(fmt.Sprintln(warning))
		}
	case 23:
		opCode, err := session.GetByte()
		if err != nil {
			return err
		}
		err = conn.getServerNetworkInformation(opCode)
		if err != nil {
			return err
		}
	case 28:
		err = conn.protocolNegotiation()
		if err != nil {
			return err
		}
		err = conn.dataTypeNegotiation()
		if err != nil {
			return err
		}
	default:
		return errors.New(fmt.Sprintf("TTC error: received code %d during response reading", msgCode))
	}
	return nil
	// cancel loop if = 4 or 9
}

func (conn *Connection) setBad() {
	conn.bad = true
}

func (conn *Connection) ResetSession(_ context.Context) error {
	if conn.bad {
		return driver.ErrBadConn
	}
	return nil
}

func (conn *Connection) dataTypeNegotiation() error {
	tracer := conn.tracer
	var err error
	tracer.Print("Data Type Negotiation")
	conn.dataNego = buildTypeNego(conn.tcpNego, conn.session)
	err = conn.dataNego.write(conn.session)
	if err != nil {
		return err
	}
	conn.dbTimeZone, err = conn.dataNego.read(conn.session)
	if err != nil {
		return err
	}
	if conn.dbTimeZone == nil {
		conn.tracer.Print("DB timezone not retrieved in data type negotiation")
		conn.tracer.Print("try to query DB timezone")
		err = conn.getDBTimeZone()
		if err != nil {
			conn.tracer.Print("error during get DB timezone: ", err)
			conn.tracer.Print("set DB timezone to: UTC(+00:00)")
			conn.dbTimeZone = time.UTC
		}
	}
	conn.tracer.Print("DB timezone: ", conn.dbTimeZone)
	conn.session.TTCVersion = conn.dataNego.CompileTimeCaps[7]
	conn.session.UseBigScn = conn.tcpNego.ServerCompileTimeCaps[7] >= 8
	if conn.tcpNego.ServerCompileTimeCaps[7] < conn.session.TTCVersion {
		conn.session.TTCVersion = conn.tcpNego.ServerCompileTimeCaps[7]
	}
	tracer.Print("TTC Version: ", conn.session.TTCVersion)
	if len(conn.tcpNego.ServerRuntimeCaps) > 6 && conn.tcpNego.ServerRuntimeCaps[6]&4 == 4 {
		conn.maxLen.varchar = 0x7FFF
		conn.maxLen.nvarchar = 0x7FFF
		conn.maxLen.raw = 0x7FFF
	} else {
		conn.maxLen.varchar = 0xFA0
		conn.maxLen.nvarchar = 0xFA0
		conn.maxLen.raw = 0xFA0
	}
	return nil
	// this.m_b32kTypeSupported = this.m_dtyNeg.m_b32kTypeSupported;
	// this.m_bSupportSessionStateOps = this.m_dtyNeg.m_bSupportSessionStateOps;
	// this.m_marshallingEngine.m_bServerUsingBigSCN = this.m_serverCompileTimeCapabilities[7] >= (byte) 8;
}

func (conn *Connection) protocolNegotiation() error {
	tracer := conn.tracer
	var err error
	tracer.Print("TCP Negotiation")
	conn.tcpNego, err = newTCPNego(conn.session)
	if err != nil {
		return err
	}
	tracer.Print("Server Charset: ", conn.tcpNego.ServerCharset)
	tracer.Print("Server National Charset: ", conn.tcpNego.ServernCharset)
	// create string converter object
	if conn.sStrConv == nil {
		conn.sStrConv = converters.NewStringConverter(conn.tcpNego.ServerCharset)
		if conn.sStrConv == nil {
			return fmt.Errorf("the server use charset with id: %d which is not supported by the driver", conn.tcpNego.ServerCharset)
		}
	}
	conn.session.StrConv = conn.sStrConv
	if conn.nStrConv == nil {
		conn.nStrConv = converters.NewStringConverter(conn.tcpNego.ServernCharset)
		if conn.nStrConv == nil {
			return fmt.Errorf("the server use ncharset with id: %d which is not supported by the driver", conn.tcpNego.ServernCharset)
		}
	}
	conn.tcpNego.ServerFlags |= 2
	return nil
}
