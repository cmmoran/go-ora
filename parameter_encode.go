package go_ora

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/cmmoran/go-ora/v2/converters"
)

var (
	valuerType  = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
	scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
)

func (par *ParameterInfo) setDataType(conn *Connection, goType reflect.Type, data driver.Value) error {
	if par.DataType > 0 {
		return nil
	}
	// step to find the data type
	// 1- check for nil
	if goType == nil {
		par.DataType = NCHAR
		return nil
	}
	for goType.Kind() == reflect.Ptr {
		goType = goType.Elem()
	}

	// 2- check for common types
	if tNumber(goType) || tNullNumber(goType) {
		if goType.Implements(valuerType) || reflect.PointerTo(goType).Implements(valuerType) {
			// Create a zero value of this type to call Value()
			v := reflect.New(goType).Interface()
			if goType.Implements(scannerType) || reflect.PointerTo(goType).Implements(scannerType) {
				err := v.(sql.Scanner).Scan(data)
				_ = err
			}
			valuer, _ := v.(driver.Valuer)
			if valuer != nil {
				if val, err := valuer.Value(); err == nil && val != nil {
					switch val.(type) {
					case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
						par.DataType = NUMBER
						par.MaxLen = converters.MAX_LEN_NUMBER
					case bool:
						par.DataType = NUMBER
						par.MaxLen = converters.MAX_LEN_NUMBER
					case string:
						par.DataType = NCHAR
						par.CharsetForm = 1
						par.ContFlag = 16
						par.CharsetID = conn.getDefaultCharsetID()
						par.iPrimValue = v
					case []byte:
						par.DataType = RAW
						par.MaxLen = len(val.([]byte))
					case time.Time:
						par.DataType = DATE
					default:
						// Fallback: treat as string representation
						par.DataType = NCHAR
					}
					return nil
				}
			}
		}
		par.DataType = NUMBER
		par.MaxLen = converters.MAX_LEN_NUMBER
		return nil
	}
	if b, ok := tUUIDLike(data); ok {
		par.DataType = RAW
		par.MaxLen = len(b)
		par.iPrimValue = b
		return nil
	}
	switch goType {
	case tyString, tyNullString:
		par.DataType = NCHAR
		par.CharsetForm = 1
		par.ContFlag = 16
		par.CharsetID = conn.getDefaultCharsetID()
		return nil
	case tyTime, tyNullTime:
		if par.Flag&0x40 > 0 {
			par.DataType = DATE
			par.MaxLen = converters.MAX_LEN_DATE
		} else {
			par.DataType = TimeStampTZ_DTY
			par.MaxLen = converters.MAX_LEN_TIMESTAMP
		}
		return nil
	case tyBytes:
		par.DataType = RAW
		return nil
	}
	// 3- call getValue
	vData, err := getValue(data)
	if err != nil {
		return err
	}
	// 4- call setType again
	if reflect.TypeOf(vData) != reflect.TypeOf(data) {
		return par.setDataType(conn, reflect.TypeOf(vData), vData)
	}
	value := reflect.ValueOf(data)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		data = reflect.New(goType).Interface()
	}
	if temp, ok := data.(OracleTypeInterface); ok {
		err := temp.SetDataType(conn, par)
		return err
	}
	switch goType.Kind() {
	case reflect.Array, reflect.Slice:
		var inVal driver.Value = nil
		var err error
		rValue := reflect.ValueOf(data)
		size := rValue.Len()
		if size > 0 && rValue.Index(0).CanInterface() {
			inVal, err = getValue(rValue.Index(0).Interface())
			if err != nil {
				return err
			}
		}
		par.Flag = 0x43
		err = par.setDataType(conn, goType.Elem(), inVal)
		if err != nil {
			return err
		}
		if par.DataType == XMLType {
			// par.cusType is for item I should get that of array
			found := false
			for _, cust := range conn.cusTyp {
				if cust.isArray && len(cust.attribs) > 0 {
					if par.cusType.name == cust.attribs[0].cusType.name {
						found = true
						// par.TypeName = name
						par.ToID = cust.toid
						*par.cusType = cust
						par.Flag = 0x3
						break
					}
				}
			}
			if !found {
				return fmt.Errorf("can't get the collection of type %s", par.cusType.name)
			}
		}
		par.MaxNoOfArrayElements = 1
		return nil
	case reflect.Struct:
		// see if the struct is support valuer interface

		for _, cusTyp := range conn.cusTyp {
			if goType == cusTyp.typ {
				par.cusType = new(customType)
				*par.cusType = cusTyp
				par.ToID = cusTyp.toid
				// par.TypeName = cusTyp.name
			}
		}
		if par.cusType == nil {
			return errors.New("call register type before use user defined type (UDT)")
		}
		par.Version = 1
		par.DataType = XMLType
		par.MaxLen = 2000
	default:
		return fmt.Errorf("unsupported go type: %v", goType.Name())
	}

	return nil
}

func (par *ParameterInfo) encodeWithType(connection *Connection) error {
	var err error
	var val driver.Value
	val, err = getValue(par.Value)
	if err != nil {
		return err
	}
	if val == nil {
		par.IsNull = true
		par.iPrimValue = nil
		return nil
	}
	// check if array
	// if par.MaxNoOfArrayElements > 0 && par.cusType == nil {
	if par.MaxNoOfArrayElements > 0 {
		if !isArrayValue(val) {
			return fmt.Errorf("parameter %s require array value", par.Name)
		}
		var size int
		rValue := reflect.ValueOf(val)
		if isArrayValue(val) {
			size = rValue.Len()
		}
		if size == 0 {
			par.IsNull = true
			par.iPrimValue = nil
			return nil
		}
		if size > par.MaxNoOfArrayElements {
			par.MaxNoOfArrayElements = size
		}
		pars := make([]ParameterInfo, 0, size)
		var tempPar ParameterInfo
		for x := 0; x < size; x++ {
			if par.cusType != nil && par.cusType.isArray {
				tempPar = par.cusType.attribs[0].clone()
			} else {
				tempPar = par.clone()
			}
			if rValue.Index(x).CanInterface() {
				tempPar.Value = rValue.Index(x).Interface()
			}
			err = tempPar.encodeWithType(connection)
			if err != nil {
				return err
			}
			pars = append(pars, tempPar)
		}
		par.iPrimValue = pars
		return nil
	}
	switch par.DataType {
	case Boolean:
		par.iPrimValue, err = getBool(val)
		if err != nil {
			return err
		}
	case NUMBER:
		par.iPrimValue, err = NewNumber(val)
		if err != nil {
			return err
		}
	case NCHAR:
		tempString := getString(val)
		length := len(tempString)
		par.MaxCharLen = length
		par.iPrimValue = tempString
		if length > connection.maxLen.varchar {
			par.DataType = LongVarChar
		}
	case DATE:
		fallthrough
	case TIMESTAMP:
		fallthrough
	case TimeStampTZ_DTY:
		par.iPrimValue, err = getDate(val)
		if err != nil {
			return err
		}
	case RAW:
		var tempByte []byte
		tempByte, err = getBytes(val)
		if err != nil {
			return err
		}
		par.MaxLen = len(tempByte)
		par.iPrimValue = tempByte
		if par.MaxLen == 0 {
			par.MaxLen = 1
		}
		if par.MaxLen > connection.maxLen.raw {
			par.DataType = LongRaw
		}
	case OCIClobLocator:
		fallthrough
	case VECTOR:
		fallthrough
	case OCIBlobLocator:
		var temp *Lob
		temp, err = getLob(val, connection)
		if err != nil {
			return err
		}
		par.iPrimValue = temp
		if temp == nil {
			//if par.Direction == Input {
			//	par.DataType = NCHAR
			//}
			par.MaxLen = 1
			par.iPrimValue = nil
			par.IsNull = true
		}
	case OCIFileLocator:
		if value, ok := val.(BFile); ok {
			if value.Valid {
				if par.Direction == Input && !value.isInit() {
					return errors.New("BFile should be initialized first")
				}
				par.iPrimValue = &value
			} else {
				par.iPrimValue = nil
				par.IsNull = true
			}
		}
	case REFCURSOR:
		par.iPrimValue = nil
		par.IsNull = true
	case XMLType:
		rValue := reflect.ValueOf(val)
		pars := make([]ParameterInfo, 0, 10)
		// if value is null or value is not struct ==> pass null for the object
		if !rValue.IsValid() || rValue.Kind() != reflect.Struct || (rValue.Kind() == reflect.Ptr && rValue.IsNil()) {
			par.IsNull = true
			par.iPrimValue = nil
			return nil
		}
		for _, attrib := range par.cusType.attribs {
			attrib.Direction = par.Direction
			attrib.parent = par
			if fieldIndex, ok := par.cusType.fieldMap[attrib.Name]; ok {
				if rValue.Field(fieldIndex).CanInterface() {
					attrib.Value = rValue.Field(fieldIndex).Interface()
				}
				if attrib.cusType != nil && attrib.cusType.isArray {
					attrib.MaxNoOfArrayElements = 1
				}
				err = attrib.encodeWithType(connection)
				if err != nil {
					return err
				}
				pars = append(pars, attrib)
			}
		}
		par.iPrimValue = pars
	}
	return nil
}

func (par *ParameterInfo) encodePrimValue(conn *Connection) error {
	var err error
	switch value := par.iPrimValue.(type) {
	case nil:
		if par.DataType == XMLType && par.IsNull {
			if par.cusType.isArray {
				par.BValue = []byte{0xFF}
			} else {
				par.BValue = []byte{0xFD}
			}
			par.MaxNoOfArrayElements = 0
			par.Flag = 0x3
		} else {
			par.BValue = nil
		}
	// case float64:
	//	par.BValue, err = converters.EncodeDouble(value)
	//	if err != nil {
	//		return err
	//	}
	// case int64:
	//	par.BValue = converters.EncodeInt64(value)
	// case uint64:
	//	par.BValue = converters.EncodeUint64(value)
	case *Number:
		par.BValue = value.data
	case bool:
		par.BValue = converters.EncodeBool(value)
	case string:
		conv, err := conn.getStrConv(par.CharsetID)
		if err != nil {
			return err
		}
		par.BValue = conv.Encode(value)
		par.MaxLen = len(par.BValue)
		if par.MaxLen == 0 {
			par.MaxLen = 1
		}
	case time.Time:
		switch par.DataType {
		case DATE:
			par.BValue = converters.EncodeDate(value)
		case TIMESTAMP:
			par.BValue = converters.EncodeTimeStamp(value, false, true, 9)
		case TimeStampTZ_DTY:
			par.BValue = converters.EncodeTimeStamp(value, true, conn.dataNego.serverTZVersion > 0 && conn.dataNego.clientTZVersion != conn.dataNego.serverTZVersion, 9)
		case TimeStampLTZ_DTY:
			// TIMESTAMP WITH LOCAL TIME ZONE
			// Oracle stores in DBTZ, shown in session TZ
			// send UTC; DB normalizes to its TZ
			par.BValue = converters.EncodeTimeStamp(value.UTC(), false, true, 9)
		}
	case *Lob:
		par.BValue = value.sourceLocator
	case *BFile:
		par.BValue = value.lob.sourceLocator
	case []byte:
		par.BValue = value
	case []ParameterInfo:
		session := conn.session
		if par.MaxNoOfArrayElements > 0 {

			if len(value) > 0 {
				arrayBuffer := bytes.Buffer{}
				if par.DataType == XMLType {
					arrayBuffer.Write([]byte{1, 3})
					if par.MaxNoOfArrayElements > 0xFC {
						session.WriteUint(&arrayBuffer, 0xFE, 2, true, false)
						session.WriteUint(&arrayBuffer, par.MaxNoOfArrayElements, 4, true, false)
					} else {
						session.WriteUint(&arrayBuffer, par.MaxNoOfArrayElements, 2, true, false)
					}
				} else {
					session.WriteUint(&arrayBuffer, par.MaxNoOfArrayElements, 4, true, true)
				}
				for _, attrib := range value {
					attrib.parent = nil
					err = attrib.encodePrimValue(conn)
					if err != nil {
						return err
					}
					if attrib.DataType == XMLType {
						session.WriteFixedClr(&arrayBuffer, attrib.BValue)
					} else {
						if attrib.IsNull && par.DataType == XMLType {
							arrayBuffer.WriteByte(0xff)
						} else {
							session.WriteClr(&arrayBuffer, attrib.BValue)
						}
					}
					if par.MaxCharLen < attrib.MaxCharLen {
						par.MaxCharLen = attrib.MaxCharLen
					}
					if par.MaxLen < attrib.MaxLen {
						par.MaxLen = attrib.MaxLen
					}
				}
				par.BValue = arrayBuffer.Bytes()
			}
			if par.DataType == NCHAR {
				par.MaxLen = conn.maxLen.nvarchar
				par.MaxCharLen = par.MaxLen // / converters.MaxBytePerChar(par.CharsetID)
			}
			if par.DataType == RAW {
				par.MaxLen = conn.maxLen.raw
			}
			if par.DataType == XMLType {
				par.BValue = encodeObject(session, par.BValue, true)
				par.MaxNoOfArrayElements = 0
				par.Flag = 3
			}
		} else {
			var objectBuffer bytes.Buffer
			for _, attrib := range value {
				err = attrib.encodePrimValue(conn)
				if err != nil {
					return err
				}
				if attrib.DataType == OCIFileLocator && attrib.MaxLen == 0 {
					attrib.MaxLen = 4000
				}
				switch attrib.DataType {
				case XMLType:
					if attrib.cusType.isArray {
						session.WriteFixedClr(&objectBuffer, attrib.BValue)
					} else {
						objectBuffer.Write(attrib.BValue)
					}
				//case NCHAR, CHAR, LONG, LongVarChar:
				//	session.WriteFixedClr(&objectBuffer, attrib.BValue)
				default:
					session.WriteFixedClr(&objectBuffer, attrib.BValue)
					//session.WriteClr(&objectBuffer, attrib.BValue)
				}
			}
			if par.parent == nil {
				par.BValue = encodeObject(session, objectBuffer.Bytes(), false)
			} else {
				par.BValue = objectBuffer.Bytes()
			}
		}
	default:
		return fmt.Errorf("unsupported primitive type: %v", reflect.TypeOf(par.iPrimValue).Name())
	}
	return nil
}

func (par *ParameterInfo) init() {
	par.DataType = 0
	par.Flag = 3
	par.ContFlag = 0
	par.CharsetID = 0
	par.CharsetForm = 0
	par.MaxLen = 1
	par.MaxCharLen = 0
	par.MaxNoOfArrayElements = 0
	par.BValue = nil
	par.iPrimValue = nil
	par.oPrimValue = nil
}

func (par *ParameterInfo) encodeValue(size int, connection *Connection) error {
	par.init()
	err := par.setDataType(connection, reflect.TypeOf(par.Value), par.Value)
	if err != nil {
		return err
	}
	if par.MaxNoOfArrayElements > 0 && par.MaxNoOfArrayElements < size {
		par.MaxNoOfArrayElements = size
	}
	err = par.encodeWithType(connection)
	if err != nil {
		return err
	}
	err = par.encodePrimValue(connection)
	if err != nil {
		return err
	}

	// check if the data length beyond max length for some types
	//switch par.DataType {
	//case NCHAR:
	//	if len(par.BValue) > connection.maxLen.varchar {
	//		return fmt.Errorf("passing varchar value with size: %d bigger than max size: %d", len(par.BValue), connection.maxLen.varchar)
	//	}
	//case RAW:
	//	if len(par.BValue) > connection.maxLen.raw {
	//		return fmt.Errorf("passing raw value with size: %d bigger than max size: %d", len(par.BValue), connection.maxLen.raw)
	//	}
	//}
	if par.DataType == OCIFileLocator {
		par.MaxLen = size
		if par.MaxLen == 0 {
			par.MaxLen = 4000
		}
	}
	if par.Direction != Input {
		if par.DataType == NCHAR {
			if par.MaxCharLen < size {
				par.MaxCharLen = size
			}
			conv, err := connection.getStrConv(par.CharsetID)
			if err != nil {
				return err
			}
			par.MaxLen = par.MaxCharLen * converters.MaxBytePerChar(conv.GetLangID())
		}
		if par.DataType == RAW {
			if par.MaxLen < size {
				par.MaxLen = size
			}
		}
	}

	if par.Direction == Output && !(par.DataType == XMLType) {
		par.BValue = nil
		// fix max size for each array item (non-xml arrays)
		if par.MaxNoOfArrayElements > 0 {
			switch par.DataType {
			case NCHAR:
				par.MaxLen = connection.maxLen.varchar
				par.MaxCharLen = connection.maxLen.varchar
			case RAW:
				par.MaxLen = connection.maxLen.raw
			}
		}
	}
	return nil
}
