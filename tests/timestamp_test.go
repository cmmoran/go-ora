package tests

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cmmoran/go-ora/v2/converters"
)

func TestTimestamp(t *testing.T) {

	type args struct {
		db     *sql.DB
		setup  func(*testing.T, int)
		query  func(*testing.T)
		closer func(*testing.T)
		date   time.Time
		loc    *time.Location
		where  []any
	}

	dbLocal := func(v *testing.T, date time.Time, sessionLoc *time.Location, where ...any) args {
		expect := func(vv *testing.T, i int, x any) {
			rv := reflect.ValueOf(x)
			x = rv.Elem().Interface()
			switch i {
			case 0:
				exp := converters.ToDate(date, converters.WithLocation(x.(time.Time).Location()))
				require.EqualValuesf(vv, exp, x, "date[%d] value expected %v and got %v", i, exp, x)
			case 1:
				exp := converters.ToTimestamp(date, converters.WithLocation(x.(time.Time).Location()))
				require.EqualValuesf(vv, exp, x, "date[%d] value expected %v and got %v", i, exp, x)
			case 2:
				exp := date.In(x.(time.Time).Location())
				require.EqualValuesf(vv, exp, x, "date[%d] value expected %v and got %v", i, exp, x)
			default:
				exp := converters.ToTimestampWithLocalTimeZone(date, converters.WithLocation(x.(time.Time).Location()))
				require.EqualValuesf(vv, exp, x, "date[%d] value expected %v and got %v", i, exp, x)
			}
		}
		db, err := getDB()
		require.NoError(v, err)
		setup := func(vv *testing.T, i int) {
			_, _ = db.Exec(`DROP TABLE TTB_TIME PURGE`)
			_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET TIME_ZONE = '%s'`, sessionLoc.String()))
			require.NoError(vv, err)
			_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET NLS_TIME_FORMAT = '%s'`, converters.NlsTimeFormat))
			require.NoError(vv, err)
			_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET NLS_TIME_TZ_FORMAT = '%s'`, converters.NlsTimeTzFormat))
			require.NoError(vv, err)
			_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET NLS_DATE_FORMAT = '%s'`, converters.NlsDateFormat))
			require.NoError(vv, err)
			_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '%s'`, converters.NlsTimestampFormat))
			require.NoError(vv, err)
			_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = '%s'`, converters.NlsTimestampTzFormat))
			require.NoError(vv, err)
			_, err = db.Exec(`CREATE TABLE TTB_TIME(
    ID NUMBER,
    DATE1 DATE,
    DATE2 TIMESTAMP(6),
    DATE3 TIMESTAMP(6) WITH TIME ZONE,
    DATE4 TIMESTAMP(6) WITH LOCAL TIME ZONE
)`)
			_, err = db.Exec(`INSERT INTO TTB_TIME(ID, DATE1, DATE2, DATE3, DATE4) VALUES(:1, :2, :3, :4, :5)`, i, date, date, date, date)

			require.NoError(vv, err)
		}
		query := func(vv *testing.T) {
			dest := make([]any, 4)
			for j := 0; j < len(dest); j++ {
				dest[j] = new(time.Time)
			}
			if len(where) == 0 {
				err = db.QueryRow(`SELECT DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME`).Scan(dest...)
				for j := 0; j < len(dest); j++ {
					expect(vv, j, dest[j])
				}
				_, err = db.Exec(`TRUNCATE TABLE TTB_TIME`)
			} else {
				switch where[0].(string)[0:5] {
				case "DATE1":
					where[1] = converters.ToDate(where[1].(time.Time), converters.WithLocation(sessionLoc))
				case "DATE2":
					where[1] = converters.ToTimestamp(where[1].(time.Time), converters.WithLocation(sessionLoc))
				case "DATE3":
				case "DATE4":
					where[1] = converters.ToTimestampWithLocalTimeZone(where[1].(time.Time), converters.WithLocation(sessionLoc))

				}
				err = db.QueryRow(fmt.Sprintf(`SELECT DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME WHERE %s`, where[0]), where[1]).Scan(dest...)
				for j := 0; j < len(dest); j++ {
					expect(vv, j, dest[j])
				}
				_, err = db.Exec(`TRUNCATE TABLE TTB_TIME`)
			}
		}
		closer := func(vv *testing.T) {
			_, err = db.Exec(`DROP TABLE TTB_TIME PURGE`)
			require.NoError(vv, err)
			err = db.Close()
			require.NoError(vv, err)
		}
		return args{
			db:     db,
			setup:  setup,
			query:  query,
			closer: closer,
			date:   date,
			loc:    sessionLoc,
			where:  where,
		}
	}
	tests := []struct {
		name string
		args func(*testing.T) args
	}{
		{
			name: "test session_timezone=UTC",
			args: func(v *testing.T) args {
				return dbLocal(v, time.Now().Truncate(time.Microsecond), must(time.LoadLocation("UTC")))
			},
		},
		{
			name: "test session_timezone=America/New_York",
			args: func(v *testing.T) args {
				date := time.Now().Truncate(time.Microsecond)
				return dbLocal(v, date, must(time.LoadLocation("America/New_York")))
			},
		},
		{
			name: "test where=date",
			args: func(v *testing.T) args {
				date := time.Now().Truncate(time.Microsecond)
				return dbLocal(v, date, must(time.LoadLocation("UTC")), "DATE1 = :1", date)
			},
		},
		{
			name: "test where=date with session_timezone=America/New_York",
			args: func(v *testing.T) args {
				date := time.Now().Truncate(time.Microsecond)
				return dbLocal(v, date, must(time.LoadLocation("America/New_York")), "DATE1 = :1", date)
			},
		},
		{
			name: "test where=timestamp",
			args: func(v *testing.T) args {
				date := time.Now().Truncate(time.Microsecond)
				return dbLocal(v, date, must(time.LoadLocation("UTC")), "DATE2 = :1", date)
			},
		},
		{
			name: "test where=timestamptz",
			args: func(v *testing.T) args {
				date := time.Now().Truncate(time.Microsecond)
				return dbLocal(v, date, must(time.LoadLocation("UTC")), "DATE3 = :1", date)
			},
		},
		{
			name: "test where=timestampltz",
			args: func(v *testing.T) args {
				date := time.Now().Truncate(time.Microsecond)
				return dbLocal(v, date, must(time.LoadLocation("UTC")), "DATE4 = :1", date)
			},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(v *testing.T) {
			ta := tt.args(v)
			ta.setup(v, i)
			ta.query(v)
			ta.closer(v)
		})
	}
}

func must[T any](x T, err error) T {
	if err != nil {
		panic(err)
	}
	return x
}
