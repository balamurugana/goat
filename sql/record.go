/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sql

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
)

// Record - is a map of column name and its value.
type Record struct {
	alias        string
	indexNameMap map[int64]string
	nameValueMap map[string]*Value
	index        int64
}

// String - returns string representation of this record.
func (r *Record) String() string {
	var items []string
	r.Range(func(name string, value *Value) bool {
		items = append(items, name+":"+fmt.Sprintf("%v", value))
		return true
	})
	return fmt.Sprintf("{%v}", strings.Join(items, ", "))
}

// MarshalCSV - encodes to CSV data.
func (r *Record) MarshalCSV(fieldDelimiter rune) ([]byte, error) {
	var csvRecord []string
	r.Range(func(name string, value *Value) bool {
		csvRecord = append(csvRecord, value.CSVString())
		return true
	})

	buf := new(bytes.Buffer)
	w := csv.NewWriter(buf)
	w.Comma = fieldDelimiter
	if err := w.Write(csvRecord); err != nil {
		return nil, err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}

	data := buf.Bytes()
	return data[:len(data)-1], nil
}

// MarshalJSON - encodes to JSON data.
func (r *Record) MarshalJSON() ([]byte, error) {
	rv := make(map[string]interface{})
	for name, value := range r.nameValueMap {
		tokens := strings.Split(name, ".")

		if len(tokens) == 1 {
			rv[tokens[0]] = value.Value()
			continue
		}

		var leaf map[string]interface{}
		for i := range tokens[:len(tokens)-1] {
			if leafValue, ok := rv[tokens[i]]; ok {
				leaf = leafValue.(map[string]interface{})
			} else {
				leaf = make(map[string]interface{})
				rv[tokens[i]] = leaf
			}
		}

		leaf[tokens[len(tokens)-1]] = value.Value()
	}

	return json.Marshal(rv)
}

// UnmarshalCSV - decodes CSV data to record.
func (r *Record) UnmarshalCSV(columnNames, csvRecord []string) error {
	if len(columnNames) != len(csvRecord) {
		return errMissingHeaders(fmt.Errorf("column names and csv record do not match"))
	}

	for i, name := range columnNames {
		r.Set(name, NewString(csvRecord[i]))
	}

	return nil
}

func (r *Record) populate(nameValueMap map[string]interface{}, prefix string) error {
	for name, value := range nameValueMap {
		if prefix != "" {
			name = prefix + "." + name
		}

		switch value.(type) {
		case bool:
			r.Set(name, NewBool(value.(bool)))
		case float64:
			r.Set(name, NewFloat(value.(float64)))
		case string:
			r.Set(name, NewString(value.(string)))
		case map[string]interface{}:
			if err := r.populate(value.(map[string]interface{}), name); err != nil {
				return err
			}
		default:
			return errUnrecognizedFormatException(fmt.Errorf("unsupported value type %T; %v", value, value))
		}
	}

	return nil
}

// UnmarshalJSON - decodes JSON data to record.
func (r *Record) UnmarshalJSON(data []byte) error {
	nameValueMap := make(map[string]interface{})
	if err := json.Unmarshal(data, &nameValueMap); err != nil {
		return err
	}

	return r.populate(nameValueMap, r.alias)
}

// Set - sets the value for a column name.
func (r *Record) Set(name string, value *Value) {
	if _, found := r.nameValueMap[name]; !found {
		r.indexNameMap[atomic.AddInt64(&r.index, 1)] = name
	}

	r.nameValueMap[name] = value
}

// Get - gets the value for a column name.
func (r *Record) Get(name string) (*Value, bool) {
	value, found := r.nameValueMap[name]
	return value, found
}

// Range - calls f sequentially for each column name and value present in the map.
// If f returns false, range stops the iteration.
func (r *Record) Range(f func(name string, value *Value) bool) {
	length := int64(len(r.indexNameMap))
	for i := int64(0); i < length; i++ {
		name := r.indexNameMap[i]
		if !f(name, r.nameValueMap[name]) {
			break
		}
	}
}

// NewRecord - creates new record.
func NewRecord(alias string) *Record {
	return &Record{
		indexNameMap: make(map[int64]string),
		nameValueMap: make(map[string]*Value),
		alias:        alias,
	}
}
