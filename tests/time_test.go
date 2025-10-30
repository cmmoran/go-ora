package tests

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cmmoran/go-ora/v2/converters"
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
			// this cumbersomness can be obviated by using orm plugins gorm, xorm, and hiding the implementation details
			where1 = converters.ToDateLiteral(date)
			where2 = converters.ToTimestampLiteral(date)
			where3 = converters.ToTimestampWithTimeZoneLiteral(date)
			where4 = converters.ToTimestampWithLocalTimeZoneLiteral(date.In(loc))
			// can use time.Time types if we strip the parts that won't match the underlying value to be compared
			wdate1 = converters.ToDate(date)
			wdate2 = converters.ToTimestamp(date)
			wdate3 = date
			wdate4 = converters.ToTimestampWithLocalTimeZone(date.In(time.UTC))
		)

		err := db.QueryRow("SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME").Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		require.EqualValuesf(t, converters.ToDate(date, loc), date1, "date value expected %v and got %v", converters.ToDate(date, loc), date1)
		require.EqualValuesf(t, converters.ToTimestamp(date), date2, "timestamp value expected %v and got %v", converters.ToTimestamp(date), date2)
		require.EqualValuesf(t, date.In(loc), date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, converters.ToTimestampWithLocalTimeZone(date.In(loc)), date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE1 = :1`, wdate1).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		require.EqualValuesf(t, converters.ToDate(date, loc), date1, "date value expected %v and got %v", converters.ToDate(date, loc), date1)
		require.EqualValuesf(t, converters.ToTimestamp(date), date2, "timestamp value expected %v and got %v", converters.ToTimestamp(date), date2)
		require.EqualValuesf(t, date.In(loc), date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, converters.ToTimestampWithLocalTimeZone(date.In(loc)), date4, "timestamp with local time zone expected %v and got %v", date, date4)
		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE1 = :1`, where1).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		require.EqualValuesf(t, converters.ToDate(date, loc), date1, "date value expected %v and got %v", converters.ToDate(date, loc), date1)
		require.EqualValuesf(t, converters.ToTimestamp(date), date2, "timestamp value expected %v and got %v", converters.ToTimestamp(date), date2)
		require.EqualValuesf(t, date.In(loc), date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, converters.ToTimestampWithLocalTimeZone(date.In(loc)), date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE2 = :1`, wdate2).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		require.EqualValuesf(t, converters.ToDate(date, loc), date1, "date value expected %v and got %v", converters.ToDate(date, loc), date1)
		require.EqualValuesf(t, converters.ToTimestamp(date), date2, "timestamp value expected %v and got %v", converters.ToTimestamp(date), date2)
		require.EqualValuesf(t, date.In(loc), date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, converters.ToTimestampWithLocalTimeZone(date.In(loc)), date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE2 = :1`, where2).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		require.EqualValuesf(t, converters.ToDate(date, loc), date1, "date value expected %v and got %v", converters.ToDate(date, loc), date1)
		require.EqualValuesf(t, converters.ToTimestamp(date), date2, "timestamp value expected %v and got %v", converters.ToTimestamp(date), date2)
		require.EqualValuesf(t, date.In(loc), date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, converters.ToTimestampWithLocalTimeZone(date.In(loc)), date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE3 = :1`, wdate3).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		require.EqualValuesf(t, converters.ToDate(date, loc), date1, "date value expected %v and got %v", converters.ToDate(date, loc), date1)
		require.EqualValuesf(t, converters.ToTimestamp(date), date2, "timestamp value expected %v and got %v", converters.ToTimestamp(date), date2)
		require.EqualValuesf(t, date.In(loc), date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, converters.ToTimestampWithLocalTimeZone(date.In(loc)), date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE3 = :1`, where3).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		require.EqualValuesf(t, converters.ToDate(date, loc), date1, "date value expected %v and got %v", converters.ToDate(date, loc), date1)
		require.EqualValuesf(t, converters.ToTimestamp(date), date2, "timestamp value expected %v and got %v", converters.ToTimestamp(date), date2)
		require.EqualValuesf(t, date.In(loc), date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, converters.ToTimestampWithLocalTimeZone(date.In(loc)), date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE4 = :1`, wdate4).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		require.EqualValuesf(t, converters.ToDate(date, loc), date1, "date value expected %v and got %v", converters.ToDate(date, loc), date1)
		require.EqualValuesf(t, converters.ToTimestamp(date), date2, "timestamp value expected %v and got %v", converters.ToTimestamp(date), date2)
		require.EqualValuesf(t, date.In(loc), date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, converters.ToTimestampWithLocalTimeZone(date.In(loc)), date4, "timestamp with local time zone expected %v and got %v", date, date4)

		err = db.QueryRow(`SELECT ID, DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE DATE4 = :1`, where4).Scan(&id, &date1, &date2, &date3, &date4)
		if err != nil {
			return err
		}
		require.EqualValuesf(t, converters.ToDate(date, loc), date1, "date value expected %v and got %v", converters.ToDate(date, loc), date1)
		require.EqualValuesf(t, converters.ToTimestamp(date), date2, "timestamp value expected %v and got %v", converters.ToTimestamp(date), date2)
		require.EqualValuesf(t, date.In(loc), date3, "timestamp with time zone value expected %v and got %v", date.In(loc), date3)
		require.EqualValuesf(t, converters.ToTimestampWithLocalTimeZone(date.In(loc)), date4, "timestamp with local time zone expected %v and got %v", date, date4)

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
