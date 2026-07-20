package go_ora

import (
	"database/sql/driver"
	"testing"
	"time"
)

func TestDMLUsesScalarExecutionWithoutNewBatch(t *testing.T) {
	stmt := NewStmt("INSERT INTO example (id, payload, created_at) VALUES (:1, :2, :3)", nil)
	if stmt.bulkExec {
		t.Fatal("DML statement unexpectedly started in bulk mode")
	}

	err := stmt.configureBulkExecution([]driver.NamedValue{
		{Ordinal: 1, Value: [32]byte{}},
		{Ordinal: 2, Value: []byte("payload")},
		{Ordinal: 3, Value: time.Now()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stmt.bulkExec {
		t.Fatal("scalar DML unexpectedly entered bulk mode")
	}
}

func TestDMLUsesBulkExecutionOnlyWithNewBatch(t *testing.T) {
	stmt := NewStmt("INSERT INTO example (id, name) VALUES (:1, :2)", nil)
	err := stmt.configureBulkExecution([]driver.NamedValue{
		{Ordinal: 1, Value: NewBatch([]int{1, 2})},
		{Ordinal: 2, Value: NewBatch([]string{"one", "two"})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !stmt.bulkExec {
		t.Fatal("NewBatch arguments did not enable bulk mode")
	}
}

func TestDMLRejectsMixedBatchAndScalarArguments(t *testing.T) {
	stmt := NewStmt("INSERT INTO example (id, name) VALUES (:1, :2)", nil)
	err := stmt.configureBulkExecution([]driver.NamedValue{
		{Ordinal: 1, Value: NewBatch([]int{1, 2})},
		{Ordinal: 2, Value: "not a batch"},
	})
	if err == nil || err.Error() != "bulk execution requires go_ora.NewBatch for every argument" {
		t.Fatalf("unexpected error: %v", err)
	}
}
