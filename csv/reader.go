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

package csv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/balamurugana/goat/sql"
)

func getColumnNames(record []string, alias string) []string {
	columnNames := make([]string, len(record))
	for i := range record {
		columnName := fmt.Sprintf("_%v", i)
		if alias != "" {
			columnName = alias + "." + columnName
		}
		columnNames[i] = columnName
	}

	return columnNames
}

type recordReader struct {
	reader          io.Reader
	recordDelimiter []byte
	oneByte         []byte
	useOneByte      bool
}

func (rr *recordReader) Read(p []byte) (n int, err error) {
	if rr.useOneByte {
		p[0] = rr.oneByte[0]
		rr.useOneByte = false
		n, err = rr.reader.Read(p[1:])
		n++
	} else {
		n, err = rr.reader.Read(p)
	}

	if err != nil {
		return 0, err
	}

	if string(rr.recordDelimiter) == "\n" {
		return n, nil
	}

	for {
		i := bytes.Index(p, rr.recordDelimiter)
		if i < 0 {
			break
		}

		p[i] = '\n'
		if len(rr.recordDelimiter) > 1 {
			p = append(p[:i+1], p[i+len(rr.recordDelimiter):]...)
		}
	}

	n = len(p)
	if len(rr.recordDelimiter) == 1 || p[n-1] != rr.recordDelimiter[0] {
		return n, nil
	}

	if _, err = rr.reader.Read(rr.oneByte); err != nil {
		return 0, err
	}

	if rr.oneByte[0] == rr.recordDelimiter[1] {
		p[n-1] = '\n'
		return n, nil
	}

	rr.useOneByte = true
	return n, nil
}

// Reader - CSV record reader for S3Select.
type Reader struct {
	args        *ReaderArgs
	readCloser  io.ReadCloser
	csvReader   *csv.Reader
	alias       string
	columnNames []string
}

// Read - reads single record.
func (r *Reader) Read() (*sql.Record, error) {
	csvRecord, err := r.csvReader.Read()
	if err != nil {
		if err != io.EOF {
			return nil, errCSVParsingError(err)
		}

		return nil, err
	}

	if r.columnNames == nil {
		r.columnNames = getColumnNames(csvRecord, r.alias)
	}

	record := sql.NewRecord(r.alias)
	if err = record.UnmarshalCSV(r.columnNames, csvRecord); err != nil {
		return nil, errCSVParsingError(err)
	}
	return record, nil
}

// Close - closes underlaying reader.
func (r *Reader) Close() error {
	return r.readCloser.Close()
}

// NewReader - creates new CSV reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *ReaderArgs, alias string) (*Reader, error) {
	if args.IsEmpty() {
		return nil, fmt.Errorf("empty args")
	}

	csvReader := csv.NewReader(&recordReader{
		reader:          readCloser,
		recordDelimiter: []byte(args.RecordDelimiter),
		oneByte:         []byte{0},
	})
	csvReader.Comma = []rune(args.FieldDelimiter)[0]
	csvReader.Comment = []rune(args.CommentCharacter)[0]

	r := &Reader{
		args:       args,
		readCloser: readCloser,
		csvReader:  csvReader,
		alias:      alias,
	}

	if args.FileHeaderInfo == none {
		return r, nil
	}

	record, err := csvReader.Read()
	if err != nil {
		if err != io.EOF {
			return nil, errCSVParsingError(err)
		}

		return nil, err
	}

	if args.FileHeaderInfo == use {
		if alias != "" {
			for i := range record {
				record[i] = alias + "." + record[i]
			}
		}

		r.columnNames = record
		return r, nil
	}

	// handle 'IGNORE' case
	r.columnNames = getColumnNames(record, alias)
	return r, nil
}
