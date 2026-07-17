package dbms

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

type recordingArgsExecer struct {
	args []any
}

func (execer *recordingArgsExecer) ExecContext(_ context.Context, _ string, args ...any) (sql.Result, error) {
	execer.args = args
	return nil, nil
}

func TestEnableOutputRejectsUnpinnedDatabaseAPI(t *testing.T) {
	err := EnableOutput(context.Background(), nil)
	if !errors.Is(err, ErrEnableOutputRequiresConn) {
		t.Fatalf("expected ErrEnableOutputRequiresConn, got %v", err)
	}
}

func TestNewOutputUsesClampedBufferSize(t *testing.T) {
	execer := &recordingArgsExecer{}
	output, err := newOutput(context.Background(), execer, MaxBufferSize+1)
	if err != nil {
		t.Fatal(err)
	}
	if output.bufferSize != MaxBufferSize {
		t.Fatalf("expected buffer size %d, got %d", MaxBufferSize, output.bufferSize)
	}
	if len(execer.args) != 1 || execer.args[0] != MaxBufferSize {
		t.Fatalf("expected clamped enable argument, got %v", execer.args)
	}
}

func TestOutputContextRejectsWrongContextValue(t *testing.T) {
	ctx := context.WithValue(context.Background(), KeyInContext, "invalid")
	if _, err := GetOutput(ctx); err == nil {
		t.Fatal("expected invalid context value error")
	}
	if err := DisableOutput(ctx); err == nil {
		t.Fatal("expected invalid context value error")
	}
}
