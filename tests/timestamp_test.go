package tests

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cmmoran/go-ora/v2/converters"
)

func TestTimestamp(t *testing.T) {
	sessionLoc, _ := time.LoadLocation("America/New_York")
	//sessionLoc, _ := time.LoadLocation("UTC")
	db, err := getDB()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	_, _ = db.Exec(`DROP TABLE TTB_TIME PURGE`)

	_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET TIME_ZONE = '%s'`, sessionLoc.String()))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET NLS_TIME_FORMAT = '%s'`, converters.NlsTimeFormat))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET NLS_TIME_TZ_FORMAT = '%s'`, converters.NlsTimeTzFormat))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET NLS_DATE_FORMAT = '%s'`, converters.NlsDateFormat))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '%s'`, converters.NlsTimestampFormat))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = '%s'`, converters.NlsTimestampTzFormat))
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE TTB_TIME(
    ID NUMBER,
    DATE1 DATE,
    DATE2 TIMESTAMP(6),
    DATE3 TIMESTAMP(6) WITH TIME ZONE,
    DATE4 TIMESTAMP(6) WITH LOCAL TIME ZONE
)`)
	require.NoError(t, err)
	defer func() {
		_, err = db.Exec(`DROP TABLE TTB_TIME PURGE`)
		require.NoError(t, err)
	}()

	date := time.Now().Truncate(time.Microsecond)
	type args struct {
		date  time.Time
		loc   *time.Location
		where *time.Time
	}
	tests := []struct {
		name      string
		args      args
		resultLen int
		expect    func(int, any)
	}{
		{
			name: "test",
			args: args{
				date:  date,
				loc:   sessionLoc,
				where: nil,
			},
			resultLen: 4,
			expect: func(i int, x any) {
				rv := reflect.ValueOf(x)
				x = rv.Elem().Interface()
				switch i {
				case 0:
					exp := converters.ToDate(date)
					require.EqualValuesf(t, exp, x, "date[%d] value expected %v and got %v", i, exp, x)
				case 1:
					exp := converters.ToTimestamp(date)
					require.EqualValuesf(t, exp, x, "date[%d] value expected %v and got %v", i, exp, x)
				case 2:
					exp := date.In(x.(time.Time).Location())
					require.EqualValuesf(t, exp, x, "date[%d] value expected %v and got %v", i, exp, x)
				default:
					exp := converters.ToTimestampWithLocalTimeZone(date, converters.WithLocation(x.(time.Time).Location()))
					require.EqualValuesf(t, exp, x, "date[%d] value expected %v and got %v", i, exp, x)
				}
			},
		},
	}
	for i, tt := range tests {
		_, err = db.Exec(`INSERT INTO TTB_TIME(ID, DATE1, DATE2, DATE3, DATE4) VALUES(:1, :2, :3, :4, :5)`, i, tt.args.date, tt.args.date, tt.args.date, tt.args.date)
		require.NoError(t, err)
		if tt.args.where == nil {
			dest := make([]any, tt.resultLen)
			for j := 0; j < len(dest); j++ {
				dest[j] = new(time.Time)
			}
			err = db.QueryRow(`SELECT DATE1, DATE2, DATE3, DATE4 FROM TTB_TIME`).Scan(dest...)
			for j := 0; j < tt.resultLen; j++ {
				tt.expect(j, dest[j])
			}
		}
	}
}
