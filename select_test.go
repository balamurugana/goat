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

package s3select

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
)

var requestData = []byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectRequest>
    <Expression>SELECT count(*) from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>IGNORE</FileHeaderInfo>
            <FieldDelimiter>,</FieldDelimiter>
            <QuoteCharacter>"</QuoteCharacter>
            <QuoteEscapeCharacter>"</QuoteEscapeCharacter>
            <Comments>#</Comments>
            <AllowQuotedRecordDelimiter>FALSE</AllowQuotedRecordDelimiter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
            <QuoteFields>ASNEEDED</QuoteFields>
            <FieldDelimiter>,</FieldDelimiter>
            <QuoteCharacter>"</QuoteCharacter>
            <QuoteEscapeCharacter>"</QuoteEscapeCharacter>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectRequest>
`)

var csvData = []byte(`one,two,three
10,true,"foo"
-3,false,"bar baz"
`)

type stdoutResponseWriter struct{}

func (w *stdoutResponseWriter) Header() http.Header {
	return nil
}

func (w *stdoutResponseWriter) Write(p []byte) (int, error) {
	fmt.Println(p)
	return len(p), nil
}

func (w *stdoutResponseWriter) WriteHeader(statusCode int) {
	fmt.Println(statusCode)
}

func (w *stdoutResponseWriter) Flush() {
}

func TestS3Select(t *testing.T) {
	s3Select, err := NewS3Select(bytes.NewReader(requestData))
	if err != nil {
		t.Fatal(err)
	}

	if err = s3Select.Open(func(offset, length int64) (io.ReadCloser, error) {
		return ioutil.NopCloser(bytes.NewReader(csvData)), nil
	}); err != nil {
		t.Fatal(err)
	}

	s3Select.Send(&stdoutResponseWriter{})
	s3Select.Close()
}
