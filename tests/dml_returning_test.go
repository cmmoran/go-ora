package tests

import (
	"testing"

	go_ora "github.com/cmmoran/go-ora/v2"
	"github.com/google/uuid"
)

func TestZeroRowDMLReturningPreservesUUIDOutput(t *testing.T) {
	db, err := getDB()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	if err := execCmd(db, `CREATE TABLE TTB_ZERO_ROW_RETURNING (
		ID NUMBER(10) PRIMARY KEY,
		UUID_VALUE RAW(16)
	)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := execCmd(db, "DROP TABLE TTB_ZERO_ROW_RETURNING PURGE"); err != nil {
			t.Error(err)
		}
	}()

	initial := uuid.MustParse("a40b65f9-5d1d-415c-a2ac-fea0933c8d4e")
	replacement := uuid.MustParse("1748596a-b2c8-4a97-9f56-b854f7c37e6c")
	output := initial

	result, err := db.Exec(`UPDATE TTB_ZERO_ROW_RETURNING
		SET UUID_VALUE = :1 WHERE ID = :2
		RETURNING UUID_VALUE INTO :3`, replacement[:], 404, go_ora.Out{Dest: &output, Size: 16})
	if err != nil {
		t.Fatal(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if rowsAffected != 0 {
		t.Fatalf("expected zero affected rows, got %d", rowsAffected)
	}
	if output != initial {
		t.Fatalf("zero-row DML RETURNING changed output from %s to %s", initial, output)
	}
}
