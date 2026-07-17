package go_ora

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cmmoran/go-ora/v2/converters"
)

var (
	conn      = &Connection{tcpNego: &TCPNego{ServernCharset: 870, ServerCharset: 0x230}}
	expNilPar = ParameterInfo{
		DataType: NCHAR,
		Flag:     3,
		MaxLen:   1,
	}
)

type testRaw16 [16]byte

type testUUIDValuer [16]byte

func (value testUUIDValuer) Value() (driver.Value, error) {
	return "a40b65f9-5d1d-415c-a2ac-fea0933c8d4e", nil
}

func TestEncodeUUIDLikeValuerArrayElementsAsRawBytes(t *testing.T) {
	values := []testUUIDValuer{
		{0xa4, 0x0b, 0x65, 0xf9, 0x5d, 0x1d, 0x41, 0x5c, 0xa2, 0xac, 0xfe, 0xa0, 0x93, 0x3c, 0x8d, 0x4e},
		{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00},
	}
	testConn := *conn
	testConn.maxLen.raw = 0x7FFF

	for index, value := range values {
		par := &ParameterInfo{Direction: Input, Value: value}
		if err := par.encodeValue(0, &testConn); err != nil {
			t.Fatalf("encode element %d: %v", index, err)
		}
		if par.DataType != RAW {
			t.Fatalf("element %d: expected RAW, got %v", index, par.DataType)
		}
		if !bytes.Equal(par.BValue, value[:]) {
			t.Fatalf("element %d: expected 16 UUID bytes %x, got %x", index, value, par.BValue)
		}
	}
}

func TestCheckNamedValuePreservesUUIDLikeValuer(t *testing.T) {
	value := testUUIDValuer{}
	namedValue := &driver.NamedValue{Value: value}
	if err := (&Connection{}).CheckNamedValue(namedValue); err != nil {
		t.Fatalf("connection converted UUID-like value through driver.Valuer: %v", err)
	}
	if err := (&Stmt{}).CheckNamedValue(namedValue); err != nil {
		t.Fatalf("statement converted UUID-like value through driver.Valuer: %v", err)
	}
}

func TestExplicitCharacterTypesOverrideUUIDInference(t *testing.T) {
	const uuidText = "a40b65f9-5d1d-415c-a2ac-fea0933c8d4e"
	testConn := *conn
	testConn.maxLen.varchar = 0x7FFF
	testConn.maxLen.nvarchar = 0x7FFF
	testConn.maxLen.raw = 0x7FFF
	tests := []struct {
		name        string
		value       driver.Value
		charsetForm int
	}{
		{name: "database charset", value: VarChar(uuidText), charsetForm: 1},
		{name: "national charset", value: NVarChar(uuidText), charsetForm: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			par := &ParameterInfo{Direction: Input, Value: test.value}
			if err := par.encodeValue(0, &testConn); err != nil {
				t.Fatal(err)
			}
			if par.DataType != NCHAR || par.CharsetForm != test.charsetForm {
				t.Fatalf("expected character bind with form %d, got type %v form %d", test.charsetForm, par.DataType, par.CharsetForm)
			}
			if par.iPrimValue != uuidText {
				t.Fatalf("expected text value %q, got %#v", uuidText, par.iPrimValue)
			}
		})
	}

	par := &ParameterInfo{Direction: Input, Value: uuidText}
	if err := par.encodeValue(0, &testConn); err != nil {
		t.Fatal(err)
	}
	if par.DataType != RAW || len(par.BValue) != 16 {
		t.Fatalf("ordinary UUID string should auto-bind as RAW(16), got type %v value %x", par.DataType, par.BValue)
	}
}

func checkParInfo(par *ParameterInfo, expPar *ParameterInfo) error {
	if par.CharsetForm != expPar.CharsetForm {
		return fmt.Errorf("expected charset form %v and get %v", expPar.CharsetForm, par.CharsetForm)
	}
	if par.CharsetID != expPar.CharsetID {
		return fmt.Errorf("expected charset id %v and get %v", expPar.CharsetID, par.CharsetID)
	}
	if par.DataType != expPar.DataType {
		return fmt.Errorf("expected data type %v and get %v", expPar.DataType, par.DataType)
	}
	if par.Flag != expPar.Flag {
		return fmt.Errorf("expected flag %v and get %v", expPar.Flag, par.Flag)
	}
	if par.ContFlag != expPar.ContFlag {
		return fmt.Errorf("exptected cont flag %v and get %v", expPar.ContFlag, par.ContFlag)
	}
	if par.MaxLen != expPar.MaxLen {
		return fmt.Errorf("expected max len %v and get %v", expPar.MaxLen, par.MaxLen)
	}
	if par.MaxCharLen != expPar.MaxCharLen {
		return fmt.Errorf("expected max char len %v and get %v", expPar.MaxCharLen, par.MaxCharLen)
	}

	if !reflect.DeepEqual(par.iPrimValue, expPar.iPrimValue) {
		return fmt.Errorf("expected primary values %v and get %v", expPar.iPrimValue, par.iPrimValue)
	}
	if bytes.Compare(par.BValue, expPar.BValue) != 0 {
		return fmt.Errorf("expected binary value %v and get %v", expPar.BValue, par.BValue)
	}
	return nil
}

func TestEncodeOutputUUIDLikePointerPointer(t *testing.T) {
	var id *testRaw16
	par := &ParameterInfo{
		Direction: Output,
		Value:     &id,
	}

	err := par.encodeValue(0, conn)
	if err != nil {
		t.Fatal(err)
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType: RAW,
		Flag:     3,
		MaxLen:   16,
	})
	if err != nil {
		t.Error(err)
	}
}

//func testEncodeValue(t *testing.T, title string, par *ParameterInfo, value interface{}, expType TNSType, flag, contFlag, charsetID, charsetForm, maxLen, maxCharLen int) error {
//	t.Log(title)
//	err := par.encodeValue(value, -1, conn)
//	if err != nil {
//		return err
//	}
//	err = checkParInfo(par, expType, flag, contFlag, charsetID, charsetForm, maxLen, maxCharLen)
//	if err != nil {
//		return err
//	}
//	t.Logf("value: %v", par.Value)
//	t.Logf("primitive value: %v", par.iPrimValue)
//	t.Logf("network value: %v", par.BValue)
//	t.Log()
//	return nil
//}

func TestEncodeValue(t *testing.T) {
	// test input parameters
	// test number
	par := &ParameterInfo{Direction: Input}
	var err error
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}
	err = checkParInfo(par, &ParameterInfo{
		DataType: NCHAR,
		Flag:     3,
		MaxLen:   1,
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = 5
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	var val5 *Number
	val5, err = NewNumberFromInt64(5)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType:   NUMBER,
		Flag:       3,
		MaxLen:     22,
		BValue:     []byte{193, 6},
		iPrimValue: val5,
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = 10.9
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	var val10p9 *Number
	val10p9, err = NewNumberFromFloat(10.9)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType:   NUMBER,
		Flag:       3,
		MaxLen:     22,
		iPrimValue: val10p9,
		BValue:     []byte{193, 11, 91},
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = true
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	var val1 *Number
	val1, err = NewNumberFromInt64(1)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType:   NUMBER,
		Flag:       3,
		MaxLen:     22,
		iPrimValue: val1,
		BValue:     []byte{193, 2},
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = false
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	var val0 *Number
	val0, err = NewNumberFromInt64(0)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType:   NUMBER,
		Flag:       3,
		MaxLen:     22,
		iPrimValue: val0,
		BValue:     []byte{128},
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = sql.NullBool{false, true}
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}
	err = checkParInfo(par, &ParameterInfo{
		DataType:   NUMBER,
		Flag:       3,
		MaxLen:     22,
		iPrimValue: val0,
		BValue:     []byte{128},
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = sql.NullBool{true, false}
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}
	err = checkParInfo(par, &ParameterInfo{
		DataType: NUMBER,
		Flag:     3,
		MaxLen:   22,
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = sql.NullInt32{25, true}
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	var val25 *Number
	val25, err = NewNumberFromInt64(25)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType:   NUMBER,
		Flag:       3,
		MaxLen:     22,
		iPrimValue: val25,
		BValue:     []byte{193, 26},
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = sql.NullInt32{25, false}
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}
	err = checkParInfo(par, &ParameterInfo{
		DataType: NUMBER,
		Flag:     3,
		MaxLen:   22,
	})
	if err != nil {
		t.Error(err)
		return
	}

	stringVal := "this is a test"
	par.Value = stringVal
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType:    LongVarChar,
		Flag:        3,
		ContFlag:    16,
		CharsetID:   0x230,
		CharsetForm: 1,
		MaxCharLen:  len(stringVal),
		MaxLen:      len(stringVal),
		iPrimValue:  stringVal,
		BValue:      []byte{116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116},
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = sql.NullString{stringVal, false}
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}
	err = checkParInfo(par, &ParameterInfo{
		DataType:    NCHAR,
		Flag:        3,
		ContFlag:    16,
		CharsetID:   0x230,
		CharsetForm: 1,
		MaxLen:      1,
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = NVarChar(stringVal)
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType:    LongVarChar,
		Flag:        3,
		ContFlag:    16,
		CharsetID:   870,
		CharsetForm: 2,
		MaxCharLen:  len(stringVal),
		MaxLen:      len(stringVal),
		iPrimValue:  stringVal,
		BValue:      []byte{116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116},
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = NullNVarChar{NVarChar(stringVal), false}
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}
	err = checkParInfo(par, &ParameterInfo{
		DataType:    NCHAR,
		Flag:        3,
		ContFlag:    16,
		CharsetID:   870,
		CharsetForm: 2,
		MaxLen:      1,
	})
	if err != nil {
		t.Error(err)
		return
	}

	timeVal := time.Date(2023, 5, 28, 23, 38, 11, 500, time.UTC)
	par.Value = timeVal
	conn.dataNego = &DataTypeNego{
		clientTZVersion: 1,
		serverTZVersion: 1,
	}
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType:   TimeStampTZ_DTY,
		Flag:       3,
		ContFlag:   0,
		MaxLen:     13,
		iPrimValue: timeVal,
		BValue:     converters.EncodeTimeStamp(timeVal, true, true),
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = sql.NullTime{timeVal, false}
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType: TimeStampTZ_DTY,
		Flag:     3,
		MaxLen:   13,
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = TimeStamp(timeVal)
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType:   TIMESTAMP,
		Flag:       3,
		ContFlag:   0,
		MaxLen:     converters.MAX_LEN_TIMESTAMP,
		iPrimValue: timeVal,
		BValue:     converters.EncodeTimeStamp(timeVal, false, true),
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = NullTimeStamp{TimeStamp(time.Now()), false}
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType: TIMESTAMP,
		Flag:     3,
		MaxLen:   converters.MAX_LEN_TIMESTAMP,
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = TimeStampTZ(timeVal)
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}

	err = checkParInfo(par, &ParameterInfo{
		DataType:   TimeStampTZ_DTY,
		Flag:       3,
		ContFlag:   0,
		MaxLen:     13,
		iPrimValue: timeVal,
		BValue:     converters.EncodeTimeStamp(timeVal, true, true),
	})
	if err != nil {
		t.Error(err)
		return
	}

	par.Value = NullTimeStampTZ{TimeStampTZ(time.Now()), false}
	err = par.encodeValue(-1, conn)
	if err != nil {
		t.Error(err)
		return
	}
	err = checkParInfo(par, &ParameterInfo{
		DataType: TimeStampTZ_DTY,
		Flag:     3,
		MaxLen:   13,
	})
	if err != nil {
		t.Error(err)
		return
	}
}
