package dbms

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/cmmoran/go-ora/v2"
)

type DBOutput struct {
	bufferSize int
	ctx        context.Context
	conn       outputExecer
}

type outputExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

const (
	MaxBufferSize = 0x7FFF
	MinBufferSize = 2000
	KeyInContext  = "GO-ORA.DBMS_OUTPUT"
)

var ErrEnableOutputRequiresConn = errors.New("dbms: EnableOutput cannot preserve Oracle session affinity; use EnableOutputContext with *sql.Conn")

// enable oracle output for current session
// param:
//
//	ctx: context of goroutine used in large apps
//	     for main: context.Background()
//	     for rest apis:
//	       http.Request.Context()
//	       gin.Context
//	       fiber.Ctx.Context()
//	       ...
//
// Deprecated: EnableOutput cannot return the derived context or preserve Oracle
// session affinity through *sql.DB. Use EnableOutputContext instead.
func EnableOutput(context.Context, *sql.DB) error {
	return ErrEnableOutputRequiresConn
}

// EnableOutputContext enables DBMS output on a pinned Oracle session and
// returns the context that must be passed to GetOutput and DisableOutput.
// The caller retains ownership of conn.
func EnableOutputContext(ctx context.Context, conn *sql.Conn) (context.Context, error) {
	out, err := NewOutputContext(ctx, conn, MaxBufferSize)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, KeyInContext, out), nil
}

// disable oracle output for current session
func DisableOutput(ctx context.Context) error {
	out := ctx.Value(KeyInContext)
	if out == nil {
		return fmt.Errorf("invalid context")
	}
	output, ok := out.(*DBOutput)
	if !ok {
		return fmt.Errorf("invalid DBMS output value %T", out)
	}
	err := output.Close()
	if err != nil {
		return err
	}
	return nil
}

// get oracle output for current session
func GetOutput(ctx context.Context) (string, error) {
	out := ctx.Value(KeyInContext)
	if out == nil {
		return "", fmt.Errorf("invalid context")
	}
	dbOutput, ok := out.(*DBOutput)
	if !ok {
		return "", fmt.Errorf("invalid DBMS output value %T", out)
	}
	output, err := dbOutput.GetOutput()
	if err != nil {
		return "", err
	}
	return output, nil
}

// print oracle output into StringWriter for current session
func PrintOutput(ctx context.Context, w io.StringWriter) error {
	output, err := GetOutput(ctx)
	if err != nil {
		return err
	}
	_, err = w.WriteString(output)
	return err
}

func NewOutput(conn *sql.DB, bufferSize int) (*DBOutput, error) {
	if conn == nil {
		return nil, errors.New("dbms: nil database")
	}
	return newOutput(context.Background(), conn, bufferSize)
}

// NewOutputContext enables DBMS output through a caller-owned pinned session.
func NewOutputContext(ctx context.Context, conn *sql.Conn, bufferSize int) (*DBOutput, error) {
	if conn == nil {
		return nil, errors.New("dbms: nil connection")
	}
	return newOutput(ctx, conn, bufferSize)
}

func newOutput(ctx context.Context, conn outputExecer, bufferSize int) (*DBOutput, error) {
	output := &DBOutput{
		bufferSize: bufferSize,
		ctx:        ctx,
		conn:       conn,
	}
	sqlText := `begin dbms_output.enable(:1); end;`
	if output.bufferSize > MaxBufferSize {
		output.bufferSize = MaxBufferSize
	}
	if output.bufferSize < MinBufferSize {
		output.bufferSize = MinBufferSize
	}
	_, err := output.conn.ExecContext(ctx, sqlText, output.bufferSize)
	return output, err
}

func (db_out *DBOutput) Print(w io.StringWriter) error {
	line, err := db_out.GetOutput()
	if err != nil {
		return err
	}
	_, err = w.WriteString(line)
	return err
}

func (db_out *DBOutput) GetOutput() (string, error) {
	sqlText := `declare 
	l_line varchar2(255); 
	l_done number; 
	l_buffer long; 
begin 
 loop 
 exit when length(l_buffer)+255 > :maxbytes OR l_done = 1; 
 dbms_output.get_line( l_line, l_done ); 
 if length(l_line) > 0 then
 	l_buffer := l_buffer || l_line || chr(10); 
 end if;
 end loop; 
 :done := l_done; 
 :buffer := l_buffer; 
end;`
	var (
		state  int
		output string
	)
	_, err := db_out.conn.ExecContext(db_out.ctx, sqlText, MaxBufferSize, go_ora.Out{Dest: &state},
		go_ora.Out{Dest: &output, Size: db_out.bufferSize})
	return output, err
}

func (output *DBOutput) Close() error {
	_, err := output.conn.ExecContext(output.ctx, `begin dbms_output.disable; end;`)
	return err
}
