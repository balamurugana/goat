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

package json

import (
	"encoding/json"
	"io"

	"github.com/balamurugana/goat/sql"
)

type objectReader struct {
	reader io.Reader
	opened uint
	end    bool
	p      []byte
	n      int
	i      int
	err    error
}

func (or *objectReader) Reset() {
	or.end = false
}

func (or *objectReader) Error() error {
	return or.err
}

func (or *objectReader) Read(p []byte) (n int, err error) {
	if or.err != nil || or.end {
		return 0, io.EOF
	}

	if or.p != nil {
		// FIXME: the case where len(p) < len(or.p[or.i:or.n])
		n = copy(p, or.p[or.i:or.n])
		or.i += n
		if or.i == or.n {
			or.p = nil
			or.i = 0
			or.n = 0
		}
	} else if n, err = or.reader.Read(p); err != nil {
		or.err = err
		return n, err
	}

	for i := 0; i < n; i++ {
		switch p[i] {
		case '{':
			or.opened++
		case '}':
			or.opened--
			if or.opened == 0 {
				or.end = true
				or.p = p
				or.i = i + 1
				or.n = n
				n = or.i
				return n, nil
			}
		}
	}

	return n, nil
}

// Reader - JSON record reader for S3Select.
type Reader struct {
	args         *ReaderArgs
	alias        string
	decoder      *json.Decoder
	objectReader *objectReader
	readCloser   io.ReadCloser
}

// Read - reads single record.
func (r *Reader) Read() (*sql.Record, error) {
	record := sql.NewRecord(r.alias)

	if err := r.objectReader.Error(); err != nil {
		return nil, err
	}

	r.objectReader.Reset()

	if err := r.decoder.Decode(record); err != nil {
		return nil, errJSONParsingError(err)
	}

	return record, nil
}

// Close - closes underlaying reader.
func (r *Reader) Close() error {
	return r.readCloser.Close()
}

// NewReader - creates new JSON reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *ReaderArgs, alias string) *Reader {
	objectReader := &objectReader{reader: readCloser}
	return &Reader{
		args:         args,
		alias:        alias,
		decoder:      json.NewDecoder(objectReader),
		objectReader: objectReader,
		readCloser:   readCloser,
	}
}
