package converters

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"
	"time"
)

const (
	maxConvertibleInt    = (1 << 63) - 1
	maxConvertibleNegInt = 1 << 63
)

func ToDateLiteral(date time.Time) string {
	return date.Format("2006-01-02 15:04:05")
}

func ToDate(date time.Time, loc ...*time.Location) time.Time {
	return ToTimestamp(date, loc...).Truncate(time.Second)
}

func ToTimestamp(date time.Time, loc ...*time.Location) time.Time {
	l := time.UTC
	if len(loc) > 0 {
		l = loc[0]
	}
	return time.Date(
		date.Year(),
		date.Month(),
		date.Day(),
		date.Hour(),
		date.Minute(),
		date.Second(),
		date.Nanosecond(),
		l,
	)
}

func ToTimestampLiteral(date time.Time, precision ...int) string {
	prec := 6
	if len(precision) > 0 {
		prec = precision[0]
		if prec < 0 || prec > 9 {
			prec = 6
		}
	}
	date = date.Truncate(durationForPrecision(prec))

	return date.Format("2006-01-02 15:04:05." + strings.Repeat("9", prec) + "Z")
}

func ToTimestampWithTimeZoneLiteral(date time.Time, precision ...int) string {
	prec := 6
	if len(precision) > 0 {
		prec = precision[0]
		if prec < 0 || prec > 9 {
			prec = 6
		}
	}
	date = date.Truncate(durationForPrecision(prec))

	return date.Format("2006-01-02 15:04:05." + strings.Repeat("9", prec) + "-07:00")
}

func ToTimestampWithLocalTimeZone(date time.Time, precision ...int) time.Time {
	prec := 6
	if len(precision) > 0 {
		prec = precision[0]
		if prec < 0 || prec > 9 {
			prec = 6
		}
	}
	date = date.Truncate(durationForPrecision(prec))

	return time.Date(
		date.Year(),
		date.Month(),
		date.Day(),
		date.Hour(),
		date.Minute(),
		date.Second(),
		date.Nanosecond(),
		time.UTC,
	)
}

// ToTimestampWithLocalTimeZoneLiteral return a string literal that oracle will interpret as a timestamp
// Oracle uses the NLS_TIMESTAMP_FORMAT to convert the string literal to a valid timestamp. It is important to note
// that the `time.Time` parameter must be in #time.In(time.Location) where time.Location is the session time zone
// set for this connection. This because oracle assumes the time is already set to the session timezone and then
// performs the conversion to database time zone on the result. the precision parameter is optional and defaults to
// the oracle default precision of 6. Please note that if precision is set, the date will be truncated to that precision
// before the string is created.
func ToTimestampWithLocalTimeZoneLiteral(date time.Time, precision ...int) string {
	prec := 6
	if len(precision) > 0 {
		prec = precision[0]
		if prec < 0 || prec > 9 {
			prec = 6
		}
	}
	date = date.Truncate(durationForPrecision(prec))

	return date.Format("2006-01-02 15:04:05." + strings.Repeat("9", prec) + "Z")
}

func durationForPrecision(p int) time.Duration {
	if p <= 0 {
		return time.Second
	}
	if p > 9 {
		p = 9
	}
	// Each increment in precision increases resolution by a power of ten.
	// 1s / 10^p
	d := time.Second / time.Duration(math.Pow10(p))
	return d
}

func EncodeUUIDLike(s string) ([]byte, bool) {
	if strings.ContainsRune(s, '-') {
		buf := make([]byte, 0, 32)
		for i := 0; i < len(s); i++ {
			if s[i] != '-' {
				buf = append(buf, s[i])
			}
		}
		s = string(buf)
	}

	if len(s) == 32 {
		out := make([]byte, 16)
		for i := 0; i < 16; i++ {
			h1, ok1 := fromHex(s[i*2])
			h2, ok2 := fromHex(s[i*2+1])
			if !ok1 || !ok2 {
				goto notuuid
			}
			out[i] = (h1 << 4) | h2
		}
		return out, true
	}
notuuid:
	return nil, false
}

func fromHex(r byte) (byte, bool) {
	switch {
	case '0' <= r && r <= '9':
		return r - '0', true
	case 'a' <= r && r <= 'f':
		return r - 'a' + 10, true
	case 'A' <= r && r <= 'F':
		return r - 'A' + 10, true
	}
	return 0, false
}

// EncodeDate convert time.Time into oracle representation
func EncodeDate(ti time.Time) []byte {
	ti = ti.Truncate(time.Second)
	ret := make([]byte, 7)
	ret[0] = uint8(ti.Year()/100 + 100)
	ret[1] = uint8(ti.Year()%100 + 100)
	ret[2] = uint8(ti.Month())
	ret[3] = uint8(ti.Day())
	ret[4] = uint8(ti.Hour() + 1)
	ret[5] = uint8(ti.Minute() + 1)
	ret[6] = uint8(ti.Second() + 1)
	return ret
}

func EncodeTimeStamp(ti time.Time, withTZ, sendAsLocalTime bool, precision uint8) []byte {
	value := ti
	if !sendAsLocalTime {
		value = ti.UTC()
	}
	ret := make([]byte, 11)
	ret[0] = uint8(value.Year()/100 + 100)
	ret[1] = uint8(value.Year()%100 + 100)
	ret[2] = uint8(value.Month())
	ret[3] = uint8(value.Day())
	ret[4] = uint8(value.Hour() + 1)
	ret[5] = uint8(value.Minute() + 1)
	ret[6] = uint8(value.Second() + 1)
	ns := value.Nanosecond()
	if precision < 9 {
		// 0 precision means seconds only
		if precision == 0 {
			ns = 0
		} else {
			scale := int(math.Pow10(9 - int(precision)))
			ns = (ns / scale) * scale
		}
	}
	binary.BigEndian.PutUint32(ret[7:11], uint32(ns))
	if withTZ {
		name, _ := value.Zone()
		parts, ok := ZoneNameToRegionIDParts(name)
		if ok {
			ret = append(ret, parts...)
		} else {
			_, offset := value.Zone()
			zone1 := uint8(offset/3600) + 20
			zone2 := uint8((offset/60)%60) + 60
			ret = append(ret, zone1, zone2)
		}
		if sendAsLocalTime {
			// ret[11] and ret[12] exist only if we appended zone bytes
			if len(ret) >= 13 && (ret[11]&0x80) != 0 {
				ret[12] |= 1
				if value.IsDST() {
					ret[12] |= 2
				}
			} else if len(ret) >= 12 {
				ret[11] |= 0x40
			}
		}

	}

	return ret
}

func DecodeDate(data []byte) (time.Time, error) {
	switch len(data) {
	case 7:
		return decodeOracleDate(data)
	case 11:
		return decodeOracleTimestamp(data, false, false)
	case 13:
		return decodeOracleTimestamp(data, true, false)
	default:
		return time.Time{}, fmt.Errorf("unsupported Oracle datetime length %d", len(data))
	}
}

func decodeOracleDate(b []byte) (time.Time, error) {
	year := int(b[0]-100)*100 + int(b[1]-100)
	month := time.Month(b[2])
	day := int(b[3])
	hour := int(b[4] - 1)
	minute := int(b[5] - 1)
	sec := int(b[6] - 1)
	return time.Date(year, month, day, hour, minute, sec, 0, time.UTC), nil
}

func decodeOracleTimestamp(b []byte, withTZ, isLocal bool) (time.Time, error) {
	if len(b) < 11 {
		return time.Time{}, fmt.Errorf("invalid TIMESTAMP buffer length %d", len(b))
	}
	year := int(b[0]-100)*100 + int(b[1]-100)
	month := time.Month(b[2])
	day := int(b[3])
	hour := int(b[4] - 1)
	minute := int(b[5] - 1)
	sec := int(b[6] - 1)
	nsec := int(binary.BigEndian.Uint32(b[7:11]))

	loc := time.UTC
	if withTZ {
		loc = decodeOracleZone(b)
		return time.Date(year, month, day, hour, minute, sec, nsec, loc), nil
	} else if isLocal {
		loc = time.Local
	}
	return time.Date(year, month, day, hour, minute, sec, nsec, loc), nil
}

func decodeOracleZone(b []byte) *time.Location {
	if len(b) < 13 {
		return time.UTC
	}
	b11, b12 := b[11], b[12]
	if name, ok := RawRegionIDToZoneName(b11, b12); ok {
		if loc, err := time.LoadLocation(name); err == nil {
			return loc
		}
	} else {
		hOff := int(b11&0x3F) - 20
		mOff := int(b12) - 60
		if hOff == 0 && mOff == 0 {
			return time.UTC
		}
		return time.FixedZone(fmt.Sprintf("TZ%+03d:%02d", hOff, mOff), hOff*3600+mOff*60)
	}
	return time.UTC
}

// addDigitToMantissa return the mantissa with the added digit if the carry is not
// set by the add. Othervise, return the mantissa untouched and carry = true.
func addDigitToMantissa(mantissaIn uint64, d byte) (mantissaOut uint64, carryOut bool) {
	var carry uint64
	mantissaOut = mantissaIn

	if mantissaIn != 0 {
		var over uint64
		over, mantissaOut = bits.Mul64(mantissaIn, uint64(10))
		if over != 0 {
			return mantissaIn, true
		}
	}
	mantissaOut, carry = bits.Add64(mantissaOut, uint64(d), carry)
	if carry != 0 {
		return mantissaIn, true
	}
	return mantissaOut, false
}

// FromNumber decode Oracle binary representation of numbers
// and returns mantissa, negative and exponent
// Some documentation:
//
//		https://gotodba.com/2015/03/24/how-are-numbers-saved-in-oracle/
//	 https://www.orafaq.com/wiki/Number
func FromNumber(inputData []byte) (mantissa uint64, negative bool, exponent int, mantissaDigits int, err error) {
	if len(inputData) == 0 {
		return 0, false, 0, 0, fmt.Errorf("Invalid NUMBER")
	}
	if inputData[0] == 0x80 {
		return 0, false, 0, 0, nil
	}

	negative = inputData[0]&0x80 == 0
	if negative {
		exponent = int(inputData[0]^0x7f) - 64
	} else {
		exponent = int(inputData[0]&0x7f) - 64
	}

	buf := inputData[1:]
	// When negative, strip the last byte if equal 0x66
	if negative && inputData[len(inputData)-1] == 0x66 {
		buf = inputData[1 : len(inputData)-1]
	}

	carry := false // get true when mantissa exceeds 64 bits
	firstDigitWasZero := 0

	// Loop on mantissa digits, stop with the capacity of int64 is reached
	// Beyond, digits will be lost during convertion t
	mantissaDigits = 0
	for p, digit100 := range buf {
		if p == 0 {
			firstDigitWasZero = -1
		}
		digit100--
		if negative {
			digit100 = 100 - digit100
		}

		mantissa, carry = addDigitToMantissa(mantissa, digit100/10)
		if carry {
			break
		}
		mantissaDigits++

		mantissa, carry = addDigitToMantissa(mantissa, digit100%10)
		if carry {
			break
		}
		mantissaDigits++
	}

	exponent = exponent*2 - mantissaDigits // Adjust exponent to the retrieved mantissa
	return mantissa, negative, exponent, mantissaDigits + firstDigitWasZero, nil
}

// DecodeDouble decode NUMBER as a float64
// Please note limitations Oracle NUMBER can have 38 significant digits while
// Float64 have 51 bits. Convertion can't be perfect.
func DecodeDouble(inputData []byte) float64 {
	mantissa, negative, exponent, _, err := FromNumber(inputData)
	if err != nil {
		return math.NaN()
	}
	absExponent := int(math.Abs(float64(exponent)))
	if negative {
		return -math.Round(float64(mantissa)*math.Pow10(exponent)*math.Pow10(absExponent)) / math.Pow10(absExponent)
	}
	return math.Round(float64(mantissa)*math.Pow10(exponent)*math.Pow10(absExponent)) / math.Pow10(absExponent)
}

// DecodeInt convert NUMBER to int64
// Preserve all the possible bits of the mantissa when Int is between MinInt64 and MaxInt64 range
func DecodeInt(inputData []byte) int64 {
	mantissa, negative, exponent, _, err := FromNumber(inputData)
	if err != nil || exponent < 0 {
		return 0
	}

	for exponent > 0 {
		mantissa *= 10
		exponent--
	}
	if negative && (mantissa>>63) == 0 {
		return -int64(mantissa)
	}
	return int64(mantissa)
}

// DecodeNumber decode the given NUMBER and return an interface{} that could be either an int64 or a float64
//
// If the number can be represented by an integer it returns an int64
// Othervise, it returns a float64
//
// The sql.Parse will do the match with program need.
//
// Ex When parsing a float into an int64, the driver will try to cast the float64 into the int64.
// If the float64 can't be represented by an int64, Parse will issue an error "invalid syntax"
func DecodeNumber(inputData []byte) interface{} {
	powerOfTen := [...]uint64{
		1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
		10000000000, 100000000000, 1000000000000, 10000000000000, 100000000000000,
		1000000000000000, 10000000000000000, 100000000000000000, 1000000000000000000,
		10000000000000000000,
	}

	mantissa, negative, exponent, mantissaDigits, err := FromNumber(inputData)
	if err != nil {
		return math.NaN()
	}

	if mantissaDigits == 0 {
		return int64(0)
	}

	if exponent >= 0 && exponent < len(powerOfTen) {
		// exponent = mantissaDigits - exponent
		IntMantissa := mantissa
		IntExponent := exponent
		var over uint64
		over, IntMantissa = bits.Mul64(IntMantissa, powerOfTen[IntExponent])
		if (!negative && IntMantissa > maxConvertibleInt) ||
			(negative && IntMantissa > maxConvertibleNegInt) {
			goto fallbackToFloat
		}
		if over != 0 {
			goto fallbackToFloat
		}

		if negative && (IntMantissa>>63) == 0 {
			return -int64(IntMantissa)
		}
		return int64(IntMantissa)
	}

fallbackToFloat:
	//if negative {
	//	return -float64(mantissa) * math.Pow10(exponent)
	//}
	//
	//return float64(mantissa) * math.Pow10(exponent)
	absExponent := int(math.Abs(float64(exponent)))
	if negative {
		return -math.Round(float64(mantissa)*math.Pow10(exponent)*math.Pow10(absExponent)) / math.Pow10(absExponent)
	}
	return math.Round(float64(mantissa)*math.Pow10(exponent)*math.Pow10(absExponent)) / math.Pow10(absExponent)
}

// ToNumber encode mantissa, sign and exponent as a []byte expected by Oracle
func ToNumber(mantissa []byte, negative bool, exponent int) []byte {
	if len(mantissa) == 0 {
		return []byte{128}
	}

	if exponent%2 == 0 {
		mantissa = append([]byte{'0'}, mantissa...)
	} else {
	}

	mantissaLen := len(mantissa)
	size := 1 + (mantissaLen+1)/2
	if negative && mantissaLen < 21 {
		size++
	}
	buf := make([]byte, size, size)

	for i := 0; i < mantissaLen; i += 2 {
		b := 10 * (mantissa[i] - '0')
		if i < mantissaLen-1 {
			b += mantissa[i+1] - '0'
		}
		if negative {
			b = 100 - b
		}
		buf[1+i/2] = b + 1
	}

	if negative && mantissaLen < 21 {
		buf[len(buf)-1] = 0x66
	}

	if exponent < 0 {
		exponent--
	}
	exponent = (exponent / 2) + 1
	if negative {
		buf[0] = byte(exponent+64) ^ 0x7f
	} else {
		buf[0] = byte(exponent+64) | 0x80
	}
	return buf
}

// EncodeUint64 encode an uint64 into an oracle NUMBER internal format
// Keep all significant bits of the uint64
func EncodeUint64(val uint64) []byte {
	mantissa := []byte(strconv.FormatUint(val, 10))
	exponent := len(mantissa) - 1
	trailingZeros := 0
	for i := len(mantissa) - 1; i >= 0 && mantissa[i] == '0'; i-- {
		trailingZeros++
	}
	mantissa = mantissa[:len(mantissa)-trailingZeros]
	return ToNumber(mantissa, false, exponent)
}

// EncodeInt64 encode an int64 into an oracle NUMBER internal format
// Keep all significant bits of the int64
func EncodeInt64(val int64) []byte {
	mantissa := []byte(strconv.FormatInt(val, 10))
	negative := mantissa[0] == '-'
	if negative {
		mantissa = mantissa[1:]
	}
	exponent := len(mantissa) - 1
	trailingZeros := 0
	for i := len(mantissa) - 1; i >= 0 && mantissa[i] == '0'; i-- {
		trailingZeros++
	}
	mantissa = mantissa[:len(mantissa)-trailingZeros]
	return ToNumber(mantissa, negative, exponent)
}

// EncodeInt encode a int into an oracle NUMBER internal format
func EncodeInt(val int) []byte {
	return EncodeInt64(int64(val))
}

// EncodeDouble convert a float64 into binary NUMBER representation
func EncodeDouble(num float64) ([]byte, error) {
	if num == 0.0 {
		return []byte{128}, nil
	}

	var (
		exponent int
		err      error
	)
	mantissa := []byte(strconv.FormatFloat(num, 'e', -1, 64))
	if i := bytes.Index(mantissa, []byte{'e'}); i >= 0 {
		exponent, err = strconv.Atoi(string(mantissa[i+1:]))
		if err != nil {
			return nil, err
		}
		mantissa = mantissa[:i]
	}
	negative := mantissa[0] == '-'
	if negative {
		mantissa = mantissa[1:]
	}
	if i := bytes.Index(mantissa, []byte{'.'}); i >= 0 {
		mantissa = append(mantissa[:i], mantissa[i+1:]...)
	}
	return ToNumber(mantissa, negative, exponent), nil
}
