package pgdriver

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	pgx "github.com/jackc/pgx/v4"
)

type rowValueFunc func(src []byte) (driver.Value, error)

type Rows struct {
	conn         *Conn
	rows         pgx.Rows
	valueFuncs   []rowValueFunc
	skipNext     bool
	skipNextMore bool

	columnNames []string
}

func NewRows(conn *Conn, rows pgx.Rows, skipNext, skipNextMore bool) *Rows {
	return &Rows{
		conn:         conn,
		rows:         rows,
		skipNext:     skipNext,
		skipNextMore: skipNextMore,
	}
}

func (r *Rows) Columns() []string {
	if r.columnNames == nil {
		fields := r.rows.FieldDescriptions()
		r.columnNames = make([]string, len(fields))
		for i, fd := range fields {
			r.columnNames[i] = string(fd.Name)
		}
	}

	return r.columnNames
}

// ColumnTypeDatabaseTypeName returns the database system type name. If the name is unknown the OID is returned.
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if dt, ok := r.conn.rawConn().ConnInfo().DataTypeForOID(r.rows.FieldDescriptions()[index].DataTypeOID); ok {
		return strings.ToUpper(dt.Name)
	}

	return strconv.FormatInt(int64(r.rows.FieldDescriptions()[index].DataTypeOID), 10)
}

const varHeaderSize = 4

// ColumnTypeLength returns the length of the column type if the column is a
// variable length type. If the column is not a variable length type ok
// should return false.
func (r *Rows) ColumnTypeLength(index int) (int64, bool) {
	fd := r.rows.FieldDescriptions()[index]

	switch fd.DataTypeOID {
	case pgtype.TextOID, pgtype.ByteaOID:
		return math.MaxInt64, true
	case pgtype.VarcharOID, pgtype.BPCharArrayOID:
		return int64(fd.TypeModifier - varHeaderSize), true
	default:
		return 0, false
	}
}

// ColumnTypePrecisionScale should return the precision and scale for decimal
// types. If not applicable, ok should be false.
func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	fd := r.rows.FieldDescriptions()[index]

	switch fd.DataTypeOID {
	case pgtype.NumericOID:
		mod := fd.TypeModifier - varHeaderSize
		precision = int64((mod >> 16) & 0xffff)
		scale = int64(mod & 0xffff)
		return precision, scale, true
	default:
		return 0, 0, false
	}
}

// ColumnTypeScanType returns the value type that can be used to scan types into.
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	fd := r.rows.FieldDescriptions()[index]

	switch fd.DataTypeOID {
	case pgtype.Float8OID:
		return reflect.TypeOf(float64(0))
	case pgtype.Float4OID:
		return reflect.TypeOf(float32(0))
	case pgtype.Int8OID:
		return reflect.TypeOf(int64(0))
	case pgtype.Int4OID:
		return reflect.TypeOf(int32(0))
	case pgtype.Int2OID:
		return reflect.TypeOf(int16(0))
	case pgtype.BoolOID:
		return reflect.TypeOf(false)
	case pgtype.NumericOID:
		return reflect.TypeOf(float64(0))
	case pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID:
		return reflect.TypeOf(time.Time{})
	case pgtype.ByteaOID:
		return reflect.TypeOf([]byte(nil))
	default:
		return reflect.TypeOf("")
	}
}

func (r *Rows) Close() error {
	r.rows.Close()
	return r.rows.Err()
}

func (r *Rows) Next(dest []driver.Value) error {
	ci := r.conn.rawConn().ConnInfo()
	fieldDescriptions := r.rows.FieldDescriptions()

	if r.valueFuncs == nil {
		r.valueFuncs = make([]rowValueFunc, len(fieldDescriptions))

		for i, fd := range fieldDescriptions {
			dataTypeOID := fd.DataTypeOID
			format := fd.Format

			switch fd.DataTypeOID {
			case pgtype.BoolOID:
				var d bool
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					return d, err
				}
			case pgtype.ByteaOID:
				var d []byte
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					return d, err
				}
			case pgtype.CIDOID:
				var d pgtype.CID
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			case pgtype.DateOID:
				var d pgtype.Date
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			case pgtype.Float4OID:
				var d float32
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					return float64(d), err
				}
			case pgtype.Float8OID:
				var d float64
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					return d, err
				}
			case pgtype.Int2OID:
				var d int16
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					return int64(d), err
				}
			case pgtype.Int4OID:
				var d int32
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					return int64(d), err
				}
			case pgtype.Int8OID:
				var d int64
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					return d, err
				}
			case pgtype.JSONOID:
				var d pgtype.JSON
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			case pgtype.JSONBOID:
				var d pgtype.JSONB
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			case pgtype.OIDOID:
				var d pgtype.OIDValue
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			case pgtype.TimestampOID:
				var d pgtype.Timestamp
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			case pgtype.TimestamptzOID:
				var d pgtype.Timestamptz
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			case pgtype.XIDOID:
				var d pgtype.XID
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			default:
				var d string
				scanPlan := ci.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(ci, dataTypeOID, format, src, &d)
					return d, err
				}
			}
		}
	}
	var more bool
	if r.skipNext {
		more = r.skipNextMore
		r.skipNext = false
	} else {
		more = r.rows.Next()
	}

	if !more {
		if r.rows.Err() == nil {
			return io.EOF
		} else {
			return r.rows.Err()
		}
	}

	for i, rv := range r.rows.RawValues() {
		if rv != nil {
			var err error
			dest[i], err = r.valueFuncs[i](rv)
			if err != nil {
				return fmt.Errorf("convert field %d failed: %v", i, err)
			}
		} else {
			dest[i] = nil
		}
	}

	return nil
}
