package tests

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	. "github.com/cmmoran/go-ora/v2/converters"
)

func TestTime(t *testing.T) {
	loc, _ := time.LoadLocation("UTC")
	var createTable = func(db *sql.DB) error {
		var err error
		err = errors.Join(err, execCmd(db, fmt.Sprintf(`ALTER SESSION SET TIME_ZONE = '%s'`, loc.String())))
		err = errors.Join(err, execCmd(db, `ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'`))
		err = errors.Join(err, execCmd(db, `ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9"Z"'`))
		err = errors.Join(err, execCmd(db, `ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9TZH:TZM'`))
		return errors.Join(err, execCmd(db, `
CREATE TABLE TTB_TIME(
    ID NUMBER,
    DATE1 DATE,
    DATE2 TIMESTAMP(6),
    DATE3 TIMESTAMP(6) WITH TIME ZONE,
    DATE4 TIMESTAMP(6) WITH LOCAL TIME ZONE
)`))
	}

	var dropTable = func(db *sql.DB) error { return execCmd(db, `DROP TABLE TTB_TIME PURGE`) }
	var date = time.Now().In(loc).Truncate(time.Microsecond)
	var insert = func(db *sql.DB) error {
		_, err := db.Exec("INSERT INTO TTB_TIME(ID, DATE1, DATE2, DATE3, DATE4) VALUES(:1, :2, :3, :4, :5)",
			1, date, date, date.In(loc), date)
		return err
	}
	var query = func(db *sql.DB) error {
		var (
			id                         int
			date1, date2, date3, date4 time.Time
			// we can use string literals to compare with the underlying value
			where1 = ToDateLiteral(date)
			where2 = ToTimestampLiteral(date)
			where3 = ToTimestampWithTimeZoneLiteral(date)
			where4 = ToTimestampWithLocalTimeZoneLiteral(date, WithLocation(loc))
			// we can also use time.Time types if we strip the parts that won't match the underlying value to be compared
			wdate1 = ToDate(date, WithLocation(loc))
			wdate2 = ToTimestamp(date)
			wdate3 = date.In(loc)
			wdate4 = ToTimestampWithLocalTimeZone(date, WithLocation(loc))
		)

		err := db.QueryRow("SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME").Scan(&id, &date1, &date2, &date3, &date4)
		require.NoErrorf(t, err, "error not expected")
		require.EqualValuesf(t, wdate1, date1, "date value expected %v and got %v", ToDate(date, WithLocation(loc)), date1)
		require.EqualValuesf(t, wdate2, date2, "timestamp value expected %v and got %v", ToTimestamp(date), date2)
		require.EqualValuesf(t, wdate3, date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, wdate4, date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE1 = :1`, wdate1).Scan(&id, &date1, &date2, &date3, &date4)
		require.NoErrorf(t, err, "error not expected")
		require.EqualValuesf(t, wdate1, date1, "date value expected %v and got %v", ToDate(date, WithLocation(loc)), date1)
		require.EqualValuesf(t, wdate2, date2, "timestamp value expected %v and got %v", ToTimestamp(date), date2)
		require.EqualValuesf(t, wdate3, date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, wdate4, date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE1 = :1`, where1).Scan(&id, &date1, &date2, &date3, &date4)
		require.NoErrorf(t, err, "error not expected")
		require.EqualValuesf(t, wdate1, date1, "date value expected %v and got %v", ToDate(date, WithLocation(loc)), date1)
		require.EqualValuesf(t, wdate2, date2, "timestamp value expected %v and got %v", ToTimestamp(date), date2)
		require.EqualValuesf(t, wdate3, date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, wdate4, date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE2 = :1`, wdate2).Scan(&id, &date1, &date2, &date3, &date4)
		require.NoErrorf(t, err, "error not expected")
		require.EqualValuesf(t, wdate1, date1, "date value expected %v and got %v", ToDate(date, WithLocation(loc)), date1)
		require.EqualValuesf(t, wdate2, date2, "timestamp value expected %v and got %v", ToTimestamp(date), date2)
		require.EqualValuesf(t, wdate3, date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, wdate4, date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE2 = :1`, where2).Scan(&id, &date1, &date2, &date3, &date4)
		require.NoErrorf(t, err, "error not expected")
		require.EqualValuesf(t, wdate1, date1, "date value expected %v and got %v", ToDate(date, WithLocation(loc)), date1)
		require.EqualValuesf(t, wdate2, date2, "timestamp value expected %v and got %v", ToTimestamp(date), date2)
		require.EqualValuesf(t, wdate3, date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, wdate4, date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE3 = :1`, wdate3).Scan(&id, &date1, &date2, &date3, &date4)
		require.NoErrorf(t, err, "error not expected")
		require.EqualValuesf(t, wdate1, date1, "date value expected %v and got %v", ToDate(date, WithLocation(loc)), date1)
		require.EqualValuesf(t, wdate2, date2, "timestamp value expected %v and got %v", ToTimestamp(date), date2)
		require.EqualValuesf(t, wdate3, date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, wdate4, date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE3 = :1`, where3).Scan(&id, &date1, &date2, &date3, &date4)
		require.NoErrorf(t, err, "error not expected")
		require.EqualValuesf(t, wdate1, date1, "date value expected %v and got %v", ToDate(date, WithLocation(loc)), date1)
		require.EqualValuesf(t, wdate2, date2, "timestamp value expected %v and got %v", ToTimestamp(date), date2)
		require.EqualValuesf(t, wdate3, date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, wdate4, date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE4 = :1`, wdate4).Scan(&id, &date1, &date2, &date3, &date4)
		require.NoErrorf(t, err, "error not expected")
		require.EqualValuesf(t, wdate1, date1, "date value expected %v and got %v", ToDate(date, WithLocation(loc)), date1)
		require.EqualValuesf(t, wdate2, date2, "timestamp value expected %v and got %v", ToTimestamp(date), date2)
		require.EqualValuesf(t, wdate3, date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, wdate4, date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE4 = :1`, where4).Scan(&id, &date1, &date2, &date3, &date4)
		require.NoErrorf(t, err, "error not expected")
		require.EqualValuesf(t, wdate1, date1, "date value expected %v and got %v", ToDate(date, WithLocation(loc)), date1)
		require.EqualValuesf(t, wdate2, date2, "timestamp value expected %v and got %v", ToTimestamp(date), date2)
		require.EqualValuesf(t, wdate3, date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, wdate4, date4, "timestamp with local time zone expected %v and got %v", date, date4)

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
