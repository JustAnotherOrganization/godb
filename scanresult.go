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
	"fmt"
	"strconv"
	"sync"
	"time"
)

// TODO Remove all of the `fmt.Print*`

//ScanResult represents a scan. So if results.Next() { results.scan()} is ran multiple times, there will be multiple of these.
type ScanResult struct {
	sync.RWMutex // embedded.  see http://golang.org/ref/spec#Struct_types
	FieldPtrArr  []interface{}
	FieldArr     []interface{}
	FieldCount   int64
	MapFieldToID map[string]int64
}

func (results *ScanResult) put(k string, v int64) {
	results.RLock()

	defer results.RUnlock()
	results.MapFieldToID[k] = v
}

// Get ...
func (results *ScanResult) Get(k string) interface{} {
	results.RLock()
	defer results.RUnlock()
	// TODO: check map key exist and wrapper.FieldArr boundary.
	return results.FieldArr[results.MapFieldToID[k]]
}

// PutFields ...
func (results *ScanResult) PutFields(fArr []string) {
	fCount := len(fArr)
	results.FieldArr = make([]interface{}, fCount)
	results.FieldPtrArr = make([]interface{}, fCount)
	results.MapFieldToID = make(map[string]int64, fCount)

	for k, v := range fArr {
		results.FieldPtrArr[k] = &results.FieldArr[k]
		results.put(v, int64(k))
	}
}

//Warning, not Locked.
func (results *ScanResult) getVal(key string) (interface{}, bool) {
	results.RLock()
	defer results.RUnlock()
	if _, ok := results.MapFieldToID[key]; ok {
		return results.FieldArr[results.MapFieldToID[key]], true
	}
	return nil, false
}

// GetFieldPtrArr ...
func (results *ScanResult) GetFieldPtrArr() []interface{} {
	return results.FieldPtrArr
}

// GetFieldArr ...
func (results *ScanResult) GetFieldArr() map[string]interface{} {
	m := make(map[string]interface{}, results.FieldCount)

	for k, v := range results.MapFieldToID {
		m[k] = results.FieldArr[v]
	}

	return m
}

//GetString returns a string for the specified key in the result
func (results *ScanResult) GetString(key string) string {
	if val, ok := results.getVal(key); ok {
		switch val2 := val.(type) {
		case []uint8:
			b := make([]byte, len(val2))
			for i, v := range val2 {
				b[i] = byte(v)
			}
			return string(b)
		case string:
			return val2
		case int64:
			return fmt.Sprintf("%v", val2)
		case nil:
			return ""
		default:
			return ""
		}
	}
	return ""
}

//CheckString returns a string and a bool indicating if the string was correctly parsed for the specified key in the result
func (results *ScanResult) CheckString(key string) (string, bool) {
	if val, ok := results.getVal(key); ok {
		switch val2 := val.(type) {
		case []uint8:
			b := make([]byte, len(val2))
			for i, v := range val2 {
				b[i] = byte(v)
			}
			return string(b), true
		case string:
			return val2, true
		case int64:
			return fmt.Sprintf("%v", val2), true
		case nil:
			return "", true //Return (nothing, true) because technically it's not there? This means that there technically wasn't an error.
		default:
			return "", false
		}
	}
	return "", false
}

// GetInterface ...
func (results *ScanResult) GetInterface(key string) (i interface{}) {
	var ok bool
	if i, ok = results.CheckString(key); ok {
		return
	}
	if i, ok = results.CheckInt(key); ok {
		return
	}
	if i, ok = results.CheckBool(key); ok {
		return
	}
	return
}

//GetBool returns a bool for the specified key in the result
// TODO Add more type checks.
func (results *ScanResult) GetBool(key string) bool {
	if val, ok := results.getVal(key); ok {
		if val2, ok := val.(int64); ok {
			return val2 > 0
		}
	}
	return false
}

//CheckBool returns a bool and if the bool was correctly parsed for the specified key in the result
// TODO Add more type checks.
func (results *ScanResult) CheckBool(key string) (bool, bool) {
	if val, ok := results.getVal(key); ok {
		if val2, ok := val.(int64); ok {
			return val2 > 0, true
		}

	}
	return false, false
}

//GetInt returns an int for the specified key in the result
func (results *ScanResult) GetInt(key string) int {
	if val, ok := results.getVal(key); ok {
		switch val2 := val.(type) {
		case []uint8:

			b := make([]byte, len(val2))
			for i, v := range val2 {
				b[i] = byte(v)
			}

			num, _ := strconv.Atoi(string(b))
			return num
		case string:
			i, _ := strconv.Atoi(string(val2))
			return i
		case int64:
			return int(val2)
		case nil:
			return 0
		}
	}
	return 0
}

//CheckInt returns an int and a bool indicating if the int was correctly parsed for the specified key in the result
// TODO Add more type checks.
func (results *ScanResult) CheckInt(key string) (int, bool) {
	if val, ok := results.getVal(key); ok {
		if val2, ok := val.(int64); ok {
			return int(val2), true
		}
	}
	return 0, false
}

var supportedTimes = []string{
	"2006-01-02 15:04:5",
	"2006-01-02 15:04:5 -0700 MST",
	time.RFC3339,
	time.RFC822,
	time.RFC1123,
	time.RFC1123Z,
}

func stringToTimePtr(t string) *time.Time {
	if t == "" {
		return &time.Time{}
	}
	var tim time.Time
	var err error
	for _, timeFormat := range supportedTimes {
		tim, err = time.Parse(timeFormat, t)
		if err == nil {
			return &tim
		}
	}
	return nil
}
