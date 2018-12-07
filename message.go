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
	"encoding/binary"
	"errors"
	"hash/crc32"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// A message is in the format specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-frame-overview.png
// hence the calculation is made accordingly.
func totalByteLength(headerLength, payloadLength int) int {
	return 4 + 4 + 4 + headerLength + payloadLength + 4
}

func genMessage(header, payload []byte) []byte {
	headerLength := len(header)
	payloadLength := len(payload)
	totalLength := totalByteLength(headerLength, payloadLength)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(totalLength))
	binary.Write(buf, binary.BigEndian, uint32(headerLength))
	prelude := buf.Bytes()
	binary.Write(buf, binary.BigEndian, crc32.ChecksumIEEE(prelude))
	buf.Write(header)
	if payload != nil {
		buf.Write(payload)
	}
	message := buf.Bytes()
	binary.Write(buf, binary.BigEndian, crc32.ChecksumIEEE(message))

	return buf.Bytes()
}

// Refer genRecordsHeader().
var recordsHeader = []byte{
	13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'v', 'e', 'n', 't',
	13, ':', 'c', 'o', 'n', 't', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 24, 'a', 'p', 'p', 'l', 'i', 'c', 'a', 't', 'i', 'o', 'n', '/', 'o', 'c', 't', 'e', 't', '-', 's', 't', 'r', 'e', 'a', 'm',
	11, ':', 'e', 'v', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 7, 'R', 'e', 'c', 'o', 'r', 'd', 's',
}

// newRecordsMessage - creates new Records Message which can contain a single record, partial records,
// or multiple records. Depending on the size of the result, a response can contain one or more of these messages.
//
// Header specification
// Records messages contain three headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-record.png
//
// Payload specification
// Records message payloads can contain a single record, partial records, or multiple records.
func newRecordsMessage(payload []byte) []byte {
	return genMessage(recordsHeader, payload)
}

// continuationMessage - S3 periodically sends this message to keep the TCP connection open.
// These messages appear in responses at random. The client must detect the message type and process accordingly.
//
// Header specification:
// Continuation messages contain two headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-cont.png
//
// Payload specification:
// Continuation messages have no payload.
var continuationMessage = []byte{
	0, 0, 0, 57, // total byte-length.
	0, 0, 0, 41, // headers byte-length.
	139, 161, 157, 242, // prelude crc.
	13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'v', 'e', 'n', 't', // headers.
	11, ':', 'e', 'v', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 4, 'C', 'o', 'n', 't', // headers.
	156, 134, 74, 13, // message crc.
}

// Refer genProgressHeader().
var progressHeader = []byte{
	13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'v', 'e', 'n', 't',
	13, ':', 'c', 'o', 'n', 't', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 8, 't', 'e', 'x', 't', '/', 'x', 'm', 'l',
	11, ':', 'e', 'v', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 8, 'P', 'r', 'o', 'g', 'r', 'e', 's', 's',
}

// newProgressMessage - creates new Progress Message. S3 periodically sends this message, if requested.
// It contains information about the progress of a query that has started but has not yet completed.
//
// Header specification:
// Progress messages contain three headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-progress.png
//
// Payload specification:
// Progress message payload is an XML document containing information about the progress of a request.
//   * BytesScanned => Number of bytes that have been processed before being uncompressed (if the file is compressed).
//   * BytesProcessed => Number of bytes that have been processed after being uncompressed (if the file is compressed).
//   * BytesReturned => Current number of bytes of records payload data returned by S3.
//
// For uncompressed files, BytesScanned and BytesProcessed are equal.
//
// Example:
//
// <?xml version="1.0" encoding="UTF-8"?>
// <Progress>
//   <BytesScanned>512</BytesScanned>
//   <BytesProcessed>1024</BytesProcessed>
//   <BytesReturned>1024</BytesReturned>
// </Progress>
//
func newProgressMessage(bytesScanned, bytesProcessed, bytesReturned int64) []byte {
	payload := []byte(`<?xml version="1.0" encoding="UTF-8"?><Progress><BytesScanned>` +
		strconv.FormatInt(bytesScanned, 10) + `</BytesScanned><BytesProcessed>` +
		strconv.FormatInt(bytesProcessed, 10) + `</BytesProcessed><BytesReturned>` +
		strconv.FormatInt(bytesReturned, 10) + `</BytesReturned></Stats>`)
	return genMessage(progressHeader, payload)
}

// Refer genStatsHeader().
var statsHeader = []byte{
	13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'v', 'e', 'n', 't',
	13, ':', 'c', 'o', 'n', 't', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 8, 't', 'e', 'x', 't', '/', 'x', 'm', 'l',
	11, ':', 'e', 'v', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'S', 't', 'a', 't', 's',
}

// newStatsMessage - creates new Stats Message. S3 sends this message at the end of the request.
// It contains statistics about the query.
//
// Header specification:
// Stats messages contain three headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-stats.png
//
// Payload specification:
// Stats message payload is an XML document containing information about a request's stats when processing is complete.
//   * BytesScanned => Number of bytes that have been processed before being uncompressed (if the file is compressed).
//   * BytesProcessed => Number of bytes that have been processed after being uncompressed (if the file is compressed).
//   * BytesReturned => Total number of bytes of records payload data returned by S3.
//
// For uncompressed files, BytesScanned and BytesProcessed are equal.
//
// Example:
//
// <?xml version="1.0" encoding="UTF-8"?>
// <Stats>
//      <BytesScanned>512</BytesScanned>
//      <BytesProcessed>1024</BytesProcessed>
//      <BytesReturned>1024</BytesReturned>
// </Stats>
func newStatsMessage(bytesScanned, bytesProcessed, bytesReturned int64) []byte {
	payload := []byte(`<?xml version="1.0" encoding="UTF-8"?><Stats><BytesScanned>` +
		strconv.FormatInt(bytesScanned, 10) + `</BytesScanned><BytesProcessed>` +
		strconv.FormatInt(bytesProcessed, 10) + `</BytesProcessed><BytesReturned>` +
		strconv.FormatInt(bytesReturned, 10) + `</BytesReturned></Stats>`)
	return genMessage(statsHeader, payload)
}

// endMessage - indicates that the request is complete, and no more messages will be sent.
// You should not assume that the request is complete until the client receives an End message.
//
// Header specification:
// End messages contain two headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-end.png
//
// Payload specification:
// End messages have no payload.
var endMessage = []byte{
	0, 0, 0, 56, // total byte-length.
	0, 0, 0, 40, // headers byte-length.
	193, 198, 132, 212, // prelude crc.
	13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'v', 'e', 'n', 't', // headers.
	11, ':', 'e', 'v', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 3, 'E', 'n', 'd', // headers.
	207, 151, 211, 146, // message crc.
}

// newErrorMessage - creates new Request Level Error Message. S3 sends this message if the request failed for any reason.
// It contains the error code and error message for the failure. If S3 sends a RequestLevelError message,
// it doesn't send an End message.
//
// Header specification:
// Request-level error messages contain three headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-error.png
//
// Payload specification:
// Request-level error messages have no payload.
func newErrorMessage(errorCode, errorMessage []byte) []byte {
	buf := new(bytes.Buffer)

	buf.Write([]byte{13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'r', 'r', 'o', 'r'})

	buf.Write([]byte{14, ':', 'e', 'r', 'r', 'o', 'r', '-', 'm', 'e', 's', 's', 'a', 'g', 'e', 7})
	binary.Write(buf, binary.BigEndian, uint16(len(errorMessage)))
	buf.Write(errorMessage)

	buf.Write([]byte{11, ':', 'e', 'r', 'r', 'o', 'r', '-', 'c', 'o', 'd', 'e', 7})
	binary.Write(buf, binary.BigEndian, uint16(len(errorCode)))
	buf.Write(errorCode)

	return genMessage(buf.Bytes(), nil)
}

// NewErrorMessage - creates new Request Level Error Message specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html.
func NewErrorMessage(errorCode, errorMessage string) []byte {
	return newErrorMessage([]byte(errorCode), []byte(errorMessage))
}

// messageWriter - HTTP client writer.
type messageWriter struct {
	w               http.ResponseWriter
	bytesReturned   int64
	getProgressFunc func() (int64, int64)
	dataCh          chan []byte
	DoneCh          chan struct{}
	stopCh          chan struct{}
	isStopped       uint32
	isRunning       uint32
}

func (writer *messageWriter) start() {
	go func() {
		defer func() {
			atomic.AddUint32(&writer.isRunning, 1)

			// Close DoneCh to indicate we are done.
			close(writer.DoneCh)
		}()

		write := func(data []byte) error {
			if _, err := writer.w.Write(data); err != nil {
				return err
			}

			writer.w.(http.Flusher).Flush()
			return nil
		}

		keepAliveTicker := time.NewTicker(500 * time.Millisecond)
		defer keepAliveTicker.Stop()

		if writer.getProgressFunc == nil {
			for {
				select {
				case <-writer.stopCh:
					// We are asked to stop.
					return
				case data, ok := <-writer.dataCh:
					if !ok {
						// Got read error.  Exit the goroutine.
						return
					}
					if err := write(data); err != nil {
						// Got write error to the client.  Exit the goroutine.
						return
					}
				case <-keepAliveTicker.C:
					if err := write(continuationMessage); err != nil {
						// Got write error to the client.  Exit the goroutine.
						return
					}
				}
			}
		}

		progressTicker := time.NewTicker(3 * time.Second)
		defer progressTicker.Stop()

		for {
			select {
			case <-writer.stopCh:
				// We are asked to stop.
				return
			case data, ok := <-writer.dataCh:
				if !ok {
					// Got read error.  Exit the goroutine.
					return
				}
				if err := write(data); err != nil {
					// Got write error to the client.  Exit the goroutine.
					return
				}
			case <-keepAliveTicker.C:
				if err := write(continuationMessage); err != nil {
					// Got write error to the client.  Exit the goroutine.
					return
				}
			case <-progressTicker.C:
				bytesScanned, bytesProcessed := writer.getProgressFunc()
				bytesReturned := atomic.LoadInt64(&writer.bytesReturned)
				if err := write(newProgressMessage(bytesScanned, bytesProcessed, bytesReturned)); err != nil {
					// Got write error to the client.  Exit the goroutine.
					return
				}
			}
		}
	}()
}

// close - closes underneath goroutine.
func (writer *messageWriter) close() {
	if atomic.AddUint32(&writer.isStopped, 1) == 1 {
		close(writer.stopCh)
	}
}

func (writer *messageWriter) send(data []byte) error {
	send := func() error {
		if atomic.LoadUint32(&writer.isRunning) != 0 {
			return errors.New("closed http connection")
		}

		select {
		case writer.dataCh <- data:
			return nil
		case <-writer.DoneCh:
			return errors.New("error in sending data")
		}
	}

	err := send()
	if err != nil {
		writer.close()
	}

	return err
}

func (writer *messageWriter) SendRecords(payload []byte) error {
	err := writer.send(newRecordsMessage(payload))
	if err == nil {
		atomic.AddInt64(&writer.bytesReturned, int64(len(payload)))
	}
	return err
}

func (writer *messageWriter) SendStats(bytesScanned, bytesProcessed int64) error {
	bytesReturned := atomic.LoadInt64(&writer.bytesReturned)
	err := writer.send(newStatsMessage(bytesScanned, bytesProcessed, bytesReturned))
	if err != nil {
		return err
	}

	if err = writer.send(endMessage); err == nil {
		writer.close()
	}

	return err
}

func (writer *messageWriter) SendError(errorCode, errorMessage string) error {
	err := writer.send(newErrorMessage([]byte(errorCode), []byte(errorMessage)))
	if err == nil {
		writer.close()
	}
	return err
}

// newMessageWriter - creates new HTTP client writer.
func newMessageWriter(w http.ResponseWriter, getProgressFunc func() (bytesScanned, bytesProcessed int64)) *messageWriter {
	writer := &messageWriter{
		w:               w,
		getProgressFunc: getProgressFunc,
		dataCh:          make(chan []byte),
		DoneCh:          make(chan struct{}),
		stopCh:          make(chan struct{}),
	}
	writer.start()
	return writer
}
