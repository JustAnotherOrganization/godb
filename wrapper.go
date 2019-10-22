package godb

// Copyright 2019 Just Another Organization
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	geo "github.com/paulmach/go.geo"
)

// ReflectTypes
var (
	timePType = reflect.TypeOf(&time.Time{})
	geoPoint  = reflect.TypeOf(&geo.Point{})
	geoPath   = reflect.TypeOf(&geo.Path{})
)

//NewWrapper because one less line is amazing.
func NewWrapper(database *sql.DB) Wrapper {
	var wrapper Wrapper
	wrapper.Wrap(database)
	return wrapper
}

//Wrapper wraps around querys.
type Wrapper struct {
	ScanResults []*ScanResult
	Cursor      int //Should start at 0
	CurrentScan int //Should start at -1
	Connection  *sql.DB
	Transaction *sql.Tx
	lastResult  sql.Result
}

// RowCount returns the amount of scan results.
func (wrapper *Wrapper) RowCount() int {
	if wrapper == nil {
		return 0
	}
	return len(wrapper.ScanResults)
}

// Begin starts the transaction
func (wrapper *Wrapper) Begin() (err error) {
	wrapper.Transaction, err = wrapper.Connection.Begin()
	return
}

// Commit commits the transaction
func (wrapper *Wrapper) Commit() error {
	if err := wrapper.Transaction.Commit(); err != nil {
		return err
	}
	wrapper.Transaction = nil
	return nil
}

// Revert rolls back the transaction
func (wrapper *Wrapper) Revert() error {
	if err := wrapper.Transaction.Rollback(); err != nil {
		return err
	}
	wrapper.Transaction = nil
	return nil
}

// Next is a dirty attempt at replicating scan.Next
func (wrapper *Wrapper) Next() bool {
	defer func() { wrapper.Cursor++; wrapper.CurrentScan++ }() //Each time this is ran, make the cursor indicate the next scan result.
	return wrapper.Cursor < len(wrapper.ScanResults)           //If cursor == 0, then it will be less than 1 if there is only one result.
}

// Prepare prepares the statement.
func (wrapper *Wrapper) Prepare(statement string) (*sql.Stmt, error) {
	if wrapper.Transaction != nil {
		return wrapper.Transaction.Prepare(statement)
	}
	return wrapper.Connection.Prepare(statement)
}

// Execute executes the statement with the params, and returns last inserted id, and the rows affected.
// TODO Remove TryToClose. Return error instead.
func (wrapper *Wrapper) Execute(statementString string, params ...interface{}) error {
	statement, err := wrapper.Prepare(statementString)
	if err != nil {
		return err
	}
	defer statement.Close()

	results, err := statement.Exec(params...)
	if err != nil {
		return err
	}

	wrapper.lastResult = results

	return nil
}

// GetLastInsertedID will return the last inserted ID from the last executed SQL
func (wrapper *Wrapper) GetLastInsertedID() (int, error) {
	if wrapper.lastResult == nil {
		return 0, fmt.Errorf("Must call insert before you can get the last inserted ID")
	}
	lastInserted, err := wrapper.lastResult.LastInsertId()
	return int(lastInserted), err
}

// GetRowsAffected will return the amount of rows affected from the last executed SQL
func (wrapper *Wrapper) GetRowsAffected() (int, error) {
	if wrapper.lastResult == nil {
		return 0, fmt.Errorf("Must call insert before you can get the rows affected")
	}
	lastInserted, err := wrapper.lastResult.RowsAffected()
	return int(lastInserted), err
}

//Clears the history.
func (wrapper *Wrapper) clear() {
	wrapper.ScanResults = nil
	wrapper.CurrentScan = -1
	wrapper.Cursor = 0
}

//Wrap wraps around the database to use.
func (wrapper *Wrapper) Wrap(db *sql.DB) {
	wrapper.Connection = db
	wrapper.CurrentScan = -1
}

//Query queries the query string, with whatever params given.
// TODO Remove TryToClose. Return error instead.
func (wrapper *Wrapper) Query(queryStatement string, params ...interface{}) error {
	wrapper.clear()
	statement, err := wrapper.Prepare(queryStatement)
	if err != nil {
		return err
	}
	defer statement.Close()

	results, err := statement.Query(params...)
	if err != nil {
		// TODO Debug
		return err
	}
	defer statement.Close()

	var fArr []string
	if fArr, err = results.Columns(); err != nil {
		return err
	}

	for count := 0; results.Next(); count++ {
		var result ScanResult
		result.PutFields(fArr)
		if err = results.Scan(result.GetFieldPtrArr()...); err != nil {
			return err
		}
		wrapper.ScanResults = append(wrapper.ScanResults, &result)
	}

	return nil
}

//QueryOne queries the query string, with whatever params given, gives back one value as an interface.
// TODO Remove TryToClose. Return error instead.
func (wrapper *Wrapper) QueryOne(queryStatement string, params ...interface{}) (i interface{}, err error) {
	wrapper.clear()
	statement, err := wrapper.Prepare(queryStatement)
	if err != nil {
		return
	}
	defer statement.Close()

	results, err := statement.Query(params...)
	if err != nil {
		return
	}
	defer statement.Close()

	var fArr []string
	if fArr, err = results.Columns(); err != nil || !results.Next() {
		return
	}
	if len(fArr) > 1 {
		err = errors.New("May only return one value with this function")
		return
	}

	var sr ScanResult
	sr.PutFields(fArr)
	if err = results.Scan(sr.GetFieldPtrArr()...); err != nil {
		return
	}

	i = sr.GetInterface(fArr[0])

	return
}

func (wrapper *Wrapper) getVal(key string) (interface{}, bool) {
	current, err := wrapper.Current()
	if err != nil {
		return nil, false
	}
	return current.getVal(key)
}

//Current returns the current row(ScanResult)
func (wrapper *Wrapper) Current() (*ScanResult, error) {
	wrapper.doubleCheckCurrentScanCursor()
	if len(wrapper.ScanResults) < 1 {
		return nil, errors.New("No Scan result found")
	}
	if len(wrapper.ScanResults)-1 < wrapper.CurrentScan {
		return nil, errors.New("Ran out of Scan results")
	}
	return wrapper.ScanResults[wrapper.CurrentScan], nil
}

//GetInt ...
func (wrapper *Wrapper) GetInt(key string) int {
	current, err := wrapper.Current()
	if err != nil {
		return 0
	}
	return current.GetInt(key)
}

//CheckInt ...
func (wrapper *Wrapper) CheckInt(key string) (int, bool) {
	current, err := wrapper.Current()
	if err != nil {
		return 0, false
	}
	return current.CheckInt(key)
}

//GetBool ...
func (wrapper *Wrapper) GetBool(key string) bool {
	current, err := wrapper.Current()
	if err != nil {
		return false
	}
	return current.GetBool(key)
}

//CheckBool ...
func (wrapper *Wrapper) CheckBool(key string) (bool, bool) {
	current, err := wrapper.Current()
	if err != nil {
		return false, false
	}
	return current.CheckBool(key)
}

// GetInterface ...
func (wrapper *Wrapper) GetInterface(key string) (i interface{}) {
	current, err := wrapper.Current()
	if err != nil {
		return
	}
	return current.GetInterface(key)
}

//GetString ...
func (wrapper *Wrapper) GetString(key string) string {
	current, err := wrapper.Current()
	if err != nil {
		return ""
	}
	return current.GetString(key)
}

//CheckString ...
func (wrapper *Wrapper) CheckString(key string) (string, bool) {
	current, err := wrapper.Current()
	if err != nil {
		return "", false
	}
	return current.CheckString(key)
}

//Unmarshal is a way to umarshal into an object. The field that you are unmarshaling to MUST be a string.
// TODO check the fields we have, not the fields in the struct....
func (wrapper *Wrapper) Unmarshal(i interface{}) error {
	return wrapper.unmarshal(reflect.ValueOf(i).Elem())
}

func (wrapper *Wrapper) unmarshal(v reflect.Value) error {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		tag := t.Field(i).Tag.Get("sql")
		if tag != "" {
			switch field.Kind() {
			case reflect.Int:
				ptr := field.Addr().Interface().(*int)
				*ptr = wrapper.GetInt(tag)
			// case reflect.Int64:
			// 	ptr := field.Addr().Interface().(*int64)
			// 	*ptr = wrapper.GetInt64(tag)
			case reflect.String:
				ptr := field.Addr().Interface().(*string)
				*ptr = wrapper.GetString(tag)
			case reflect.Bool:
				ptr := field.Addr().Interface().(*bool)
				*ptr = wrapper.GetBool(tag)
			case reflect.Float64:
				ptr := field.Addr().Interface().(*float64)
				*ptr, _ = strconv.ParseFloat(wrapper.GetString(tag), 64)

				// Magic Reflection
			case reflect.Slice, reflect.Array:
				i := field.Interface()
				if err := json.Unmarshal([]byte(wrapper.GetString(tag)), &i); err != nil {
					break
				}
				a2v := reflect.ValueOf(i)
				if !a2v.IsValid() {
					break
				}
				a2vLen := a2v.Len()
				if a2vLen > 0 {
					field.Set(reflect.MakeSlice(field.Type(), a2vLen, a2vLen))
					for i := 0; i < a2vLen; i++ {
						if a2v.Index(i).Elem().Type().ConvertibleTo(field.Type().Elem()) {
							field.Index(i).Set(a2v.Index(i).Elem().Convert(field.Type().Elem()))
						}
					}
				}
				// End Magic Reflection

			case reflect.Struct, reflect.Ptr:
				switch field.Type() {
				case timePType:
					ptr := field.Addr().Interface().(**time.Time)
					*ptr = stringToTimePtr(wrapper.GetString(tag))

					// Magic Geo Points....
				case geoPoint:
					val, ok := wrapper.getVal(tag)
					if ok {
						var byts []byte
						// Uh since this is so unsafe, I decided to wrap it just incase...
						// Reasons why it's unsafe:
						// - Assuming type is []uint8
						// - Assuming data type is WKT
						// - Assuming I know what the type is.
						func() {
							defer func() {
								if err := recover(); err != nil {
								}
							}()

							byts = make([]byte, len(val.([]uint8)))
							for i, v := range val.([]uint8) {
								byts[i] = byte(v)
							}
							if len(byts) > 4 {

								// ¯\_(ツ)_/¯
								byts = byts[4:]
							}

						}()
						// Lets pretend, that byts is correct and not think about it kkthx.
						ptr := field.Addr().Interface().(**geo.Point)
						*ptr = geo.NewPointFromWKB(byts)
					}

				case geoPath:
					val, ok := wrapper.getVal(tag)
					if ok {
						var byts []byte
						// Uh since this is so unsafe, I decided to wrap it just incase...
						// Reasons why it's unsafe:
						// - Assuming type is []uint8
						// - Assuming data type is WKT
						// - Assuming I know what the type is.
						func() {
							defer func() {
								if err := recover(); err != nil {
								}
							}()

							byts = make([]byte, len(val.([]uint8)))
							for i, v := range val.([]uint8) {
								byts[i] = byte(v)
							}
							if len(byts) > 4 {

								// ¯\_(ツ)_/¯
								byts = byts[4:]
							}

						}()
						// Lets pretend, that byts is correct and not think about it kkthx.
						ptr := field.Addr().Interface().(**geo.Path)
						*ptr = geo.NewPathFromWKB(byts)
					}

				}

			}
		}
	}
	return nil
}

//HasResults is used to ensure that there are results waiting. Because certain things panic some reason...
func (wrapper *Wrapper) HasResults() bool {
	return len(wrapper.ScanResults) > 0
}

//Used incase someone forgets to call wrapper.Next(). Not the best thing, but needed to avoid a panic.
func (wrapper *Wrapper) doubleCheckCurrentScanCursor() {
	if wrapper.CurrentScan == -1 {
		wrapper.CurrentScan = 0
	}
}

// UnmarshalTo ...
func (wrapper *Wrapper) UnmarshalTo(key string, i interface{}) (err error) {
	if wrapper.GetString(key) == "" {
		return
	}
	err = json.Unmarshal([]byte(wrapper.GetString(key)), i)
	if err != nil {
		return
	}
	return
}

// ArrayOfThings ...
type ArrayOfThings interface{}

// Function ...
type Function interface{}

// Unwrap will allocate the array, and unmarshal to it.
func (wrapper *Wrapper) Unwrap(array ArrayOfThings, function ...Function) (err error) {
	value := reflect.ValueOf(array)
	if value.IsNil() {
		err = errors.New("Wrapper.Unwrap: Cannot unwrap into a nil value, you probably forgot the `&`")
		return
	}

	var f interface{}
	if function != nil {
		if len(function) > 0 {
			f = function[0]
		}
	}

	value = value.Elem()
	value.Set(reflect.MakeSlice(reflect.TypeOf(array).Elem(), wrapper.RowCount(), wrapper.RowCount()))

	if wrapper.RowCount() > 0 {
		if err = validateFunction(value.Index(0), f); err != nil {
			return
		}
	} else {
		return
	}

	for i := 0; wrapper.Next(); i++ {
		val := value.Index(i)
		if val.Kind() != reflect.Struct {
			err = fmt.Errorf("Wrapper.Unwrap: unsupported type for unwrapping: `%v`\nMust use a structed object", val.Kind())
			return
		}
		if err = wrapper.unmarshal(val); err != nil {
			return
		}
		if err = injectFunction(val, f); err != nil {
			return
		}
	}

	return
}

func validateFunction(value reflect.Value, f interface{}) (err error) {
	if f == nil {
		return
	}
	if reflect.TypeOf(f).Kind() != reflect.Func {
		err = fmt.Errorf("Wrapper.Unwrap: provided function is not a function, but rather a `%v`", reflect.TypeOf(f).Kind())
		return
	}

	fun := reflect.ValueOf(f)
	if fun.Type().NumIn() != 1 {
		// unsupported number of parameters.
		err = fmt.Errorf("Wrapper.Unwrap: provided function must only have one parameter. The type of which matches that of which is being unwrapped")
		return
	}
	if fun.Type().In(0).Kind() != reflect.Ptr {
		// unsupported parameter.
		err = fmt.Errorf("Wrapper.Unwrap: Provided function's parameter is not a pointer, which means it is being passed by value which is pointless")
		return
	}

	if fun.Type().In(0) != value.Addr().Type() {
		// unsupported parameter.
		err = fmt.Errorf("Wrapper.Unwrap: provided function's parameter does not match that which is being unwrapped")
		return
	}

	return
}

func injectFunction(value reflect.Value, f interface{}) (err error) {
	// nil check just incase.
	if f == nil {
		return
	}

	reflect.ValueOf(f).Call([]reflect.Value{value.Addr()})

	return
}
