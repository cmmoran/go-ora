package tests

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cmmoran/go-ora/v2/converters"
)

func TestTime(t *testing.T) {
	var createTable = func(db *sql.DB) error {
		var err error
		err = errors.Join(err, execCmd(db, `ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'`))
		err = errors.Join(err, execCmd(db, `ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9"Z"'`))
		err = errors.Join(err, execCmd(db, `ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9TZH:TZM'`))
		return errors.Join(err, execCmd(db, `
CREATE TABLE TTB_TIME(
    ID NUMBER,
    DATE1 DATE,
    DATE2 TIMESTAMP(9),
    DATE3 TIMESTAMP(9) WITH TIME ZONE,
    DATE4 TIMESTAMP(9) WITH LOCAL TIME ZONE
)`))
	}

	var dropTable = func(db *sql.DB) error { return execCmd(db, `DROP TABLE TTB_TIME PURGE`) }
	var date = time.Now()
	loc, _ := time.LoadLocation("America/New_York")
	var insert = func(db *sql.DB) error {
		_, err := db.Exec("INSERT INTO TTB_TIME(ID, DATE1, DATE2, DATE3, DATE4) VALUES(:1, :2, :3, :4, :5)",
			1, date, date, date.In(loc), date)
		return err
	}
	var query = func(db *sql.DB) error {
		var (
			id                         int
			date1, date2, date3, date4 time.Time
		)
		err := db.QueryRow("SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME").Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		if !isEqualTime(date1, date, false) {
			return fmt.Errorf("date value expected %v and got %v", date, date1)
		}
		if !isEqualTime(date2, date, true) {
			return fmt.Errorf("timestamp value expected %v and got %v", date, date2)
		}
		if !isEqualTime(date3, date.In(loc), true) {
			return fmt.Errorf("timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		}
		if !isEqualTime(date4.In(time.Local), date, true) {
			return fmt.Errorf("timestamp with local time zone expected %v and got %v", date, date4)
		}
		where1 := converters.ToDateLiteral(date)
		where2 := converters.ToTimestampLiteral(date, 9)
		where3 := converters.ToTimestampWithTimeZoneLiteral(date, 9)
		where4 := converters.ToTimestampWithLocalTimeZoneLiteral(date, 9)
		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE1 = :1`, where1).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		if !isEqualTime(date1, date, false) {
			return fmt.Errorf("date value expected %v and got %v", date, date1)
		}
		if !isEqualTime(date2, date, true) {
			return fmt.Errorf("timestamp value expected %v and got %v", date, date2)
		}
		if !isEqualTime(date3, date.In(loc), true) {
			return fmt.Errorf("timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		}
		if !isEqualTime(date4.In(time.Local), date, true) {
			return fmt.Errorf("timestamp with local time zone expected %v and got %v", date, date4)
		}
		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE2 = :1`, where2).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		if !isEqualTime(date1, date, false) {
			return fmt.Errorf("date value expected %v and got %v", date, date1)
		}
		if !isEqualTime(date2, date, true) {
			return fmt.Errorf("timestamp value expected %v and got %v", date, date2)
		}
		if !isEqualTime(date3, date.In(loc), true) {
			return fmt.Errorf("timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		}
		if !isEqualTime(date4.In(time.Local), date, true) {
			return fmt.Errorf("timestamp with local time zone expected %v and got %v", date, date4)
		}
		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE3 = :1`, where3).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		if !isEqualTime(date1, date, false) {
			return fmt.Errorf("date value expected %v and got %v", date, date1)
		}
		if !isEqualTime(date2, date, true) {
			return fmt.Errorf("timestamp value expected %v and got %v", date, date2)
		}
		if !isEqualTime(date3, date.In(loc), true) {
			return fmt.Errorf("timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		}
		if !isEqualTime(date4.In(time.Local), date, true) {
			return fmt.Errorf("timestamp with local time zone expected %v and got %v", date, date4)
		}
		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE4 = :1`, where4).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		if !isEqualTime(date1, date, false) {
			return fmt.Errorf("date value expected %v and got %v", date, date1)
		}
		if !isEqualTime(date2, date, true) {
			return fmt.Errorf("timestamp value expected %v and got %v", date, date2)
		}
		if !isEqualTime(date3, date.In(loc), true) {
			return fmt.Errorf("timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		}
		if !isEqualTime(date4.In(time.Local), date, true) {
			return fmt.Errorf("timestamp with local time zone expected %v and got %v", date, date4)
		}
		return nil
	}
	db, err := getDB()
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	err = createTable(db)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = dropTable(db)
		if err != nil {
			t.Error(err)
		}
	}()

	err = insert(db)
	if err != nil {
		t.Error(err)
		return
	}
	err = query(db)
	if err != nil {
		t.Error(err)
		return
	}
}
