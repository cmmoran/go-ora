package tests

import (
	"fmt"
	"strings"
	"testing"

	go_ora "github.com/cmmoran/go-ora/v2"
	"github.com/google/uuid"
)

func TestUUIDRaw16RoundTrip(t *testing.T) {
	db, err := getDB()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	if err := execCmd(db, `CREATE TABLE TTB_UUID_NATIVE (
		ID NUMBER(10) PRIMARY KEY,
		UUID_VALUE RAW(16),
		TEXT_VALUE VARCHAR2(36)
	)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := execCmd(db, "DROP TABLE TTB_UUID_NATIVE PURGE"); err != nil {
			t.Error(err)
		}
	}()

	canonical := "a40b65f9-5d1d-415c-a2ac-fea0933c8d4e"
	compact := "a40b65f95d1d415ca2acfea0933c8d4e"
	want := uuid.MustParse(canonical)

	inputs := []struct {
		id    int
		value interface{}
	}{
		{1, canonical},
		{2, compact},
		{3, want},
	}
	for _, input := range inputs {
		if _, err := db.Exec(`INSERT INTO TTB_UUID_NATIVE (ID, UUID_VALUE) VALUES (:1, :2)`, input.id, input.value); err != nil {
			t.Fatalf("insert UUID input %T: %v", input.value, err)
		}
	}

	arrayValues := []uuid.UUID{
		uuid.MustParse("97b37915-4776-4c5a-9567-3f46ca840e39"),
		uuid.MustParse("9ce8a9e1-06c0-43c8-9f19-f31d94427d0e"),
	}
	if _, err := db.Exec(`INSERT INTO TTB_UUID_NATIVE (ID, UUID_VALUE) VALUES (:1, :2)`, []int{4, 5}, arrayValues); err != nil {
		t.Fatalf("insert UUID array: %v", err)
	}

	rows, err := db.Query(`SELECT ID, UUID_VALUE, UUID_VALUE, LOWER(RAWTOHEX(UUID_VALUE))
		FROM TTB_UUID_NATIVE WHERE UUID_VALUE IS NOT NULL ORDER BY ID`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	expected := map[int]uuid.UUID{1: want, 2: want, 3: want, 4: arrayValues[0], 5: arrayValues[1]}
	rowCount := 0
	for rows.Next() {
		var (
			id       int
			googleID uuid.UUID
			goOraID  go_ora.UUIDString
			rawHex   string
		)
		if err := rows.Scan(&id, &googleID, &goOraID, &rawHex); err != nil {
			t.Fatal(err)
		}
		expectedID, ok := expected[id]
		if !ok {
			t.Fatalf("unexpected row ID %d", id)
		}
		if googleID != expectedID {
			t.Fatalf("row %d: expected Google UUID %s, got %s", id, expectedID, googleID)
		}
		if goOraID.String() != expectedID.String() {
			t.Fatalf("row %d: expected go_ora UUID %s, got %s", id, expectedID, goOraID)
		}
		if rawHex != strings.ReplaceAll(expectedID.String(), "-", "") {
			t.Fatalf("row %d: unexpected RAWTOHEX value %s", id, rawHex)
		}
		rowCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if rowCount != len(expected) {
		t.Fatalf("expected %d UUID rows, got %d", len(expected), rowCount)
	}

	if _, err := db.Exec(`INSERT INTO TTB_UUID_NATIVE (ID, UUID_VALUE) VALUES (:1, :2)`, 6, uuid.NullUUID{}); err != nil {
		t.Fatal(err)
	}
	var nullID uuid.NullUUID
	if err := db.QueryRow(`SELECT UUID_VALUE FROM TTB_UUID_NATIVE WHERE ID = 6`).Scan(&nullID); err != nil {
		t.Fatal(err)
	}
	if nullID.Valid {
		t.Fatal("expected NULL RAW(16) to scan as an invalid uuid.NullUUID")
	}

	if _, err := db.Exec(`INSERT INTO TTB_UUID_NATIVE (ID, TEXT_VALUE) VALUES (:1, :2)`, 7, go_ora.VarChar(canonical)); err != nil {
		t.Fatal(err)
	}
	var textValue string
	if err := db.QueryRow(`SELECT TEXT_VALUE FROM TTB_UUID_NATIVE WHERE ID = 7`).Scan(&textValue); err != nil {
		t.Fatal(err)
	}
	if textValue != canonical {
		t.Fatalf("expected explicit character UUID %q, got %q", canonical, textValue)
	}

	returnInput := uuid.MustParse("1748596a-b2c8-4a97-9f56-b854f7c37e6c")
	var returnOutput uuid.UUID
	if _, err := db.Exec(`INSERT INTO TTB_UUID_NATIVE (ID, UUID_VALUE) VALUES (:1, :2)
		RETURNING UUID_VALUE INTO :3`, 8, returnInput, go_ora.Out{Dest: &returnOutput, Size: 16}); err != nil {
		t.Fatal(fmt.Errorf("returning RAW(16) UUID: %w", err))
	}
	if returnOutput != returnInput {
		t.Fatalf("expected returned UUID %s, got %s", returnInput, returnOutput)
	}
}
