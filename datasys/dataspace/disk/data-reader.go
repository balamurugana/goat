package disk

import (
	"errors"
	"io"
	"path"

	"github.com/balamurugana/goat/pkg/boundary"
	xos "github.com/balamurugana/goat/pkg/os"
)

type Part struct {
	ID   string `json:"id"`
	Size uint64 `json:"size"`
}

type DataInfo struct {
	Parts []Part `json:"parts"`
	Size  uint64 `json:"size"`
}

func (dataInfo DataInfo) getParts(offset, length int64) (requiredParts []Part, bytesToSkip, bytesToRead int64) {
	partSizes := make([]int64, len(dataInfo.Parts))
	for i, part := range dataInfo.Parts {
		partSizes[i] = int64(part.Size)
	}

	var startPart, endPart int64
	startPart, endPart, bytesToSkip, bytesToRead = boundary.CalcPartBoundaries(partSizes, offset, length)
	return dataInfo.Parts[startPart:endPart], bytesToSkip, bytesToRead
}

type dataReader struct {
	dataDir       string
	dataInfo      *DataInfo
	requiredParts []Part
	bytesToSkip   int64
	bytesToRead   int64
	rc            io.ReadCloser
	index         int
	err           error
}

func (dr *dataReader) Read(b []byte) (int, error) {
	if dr.err != nil {
		if errors.Is(dr.err, io.ErrUnexpectedEOF) {
			if dr.index == len(dr.requiredParts) {
				dr.err = io.EOF
			}
		}

		return 0, dr.err
	}

	if dr.rc == nil {
		partFile := dr.requiredParts[dr.index].ID + ".part"
		filename := path.Join(dr.dataDir, partFile)

		offset := int64(0)
		if dr.index == 0 {
			offset = dr.bytesToSkip
		}

		length := dr.requiredParts[dr.index].Size - uint64(offset)
		if dr.index == len(dr.requiredParts)-1 {
			length = uint64(dr.bytesToRead)
		}

		if dr.rc, dr.err = xos.OpenFile(filename, offset, length, true); dr.err != nil {
			return 0, dr.err
		}

		dr.index++
	}

	var n int

	if n, dr.err = dr.rc.Read(b); dr.err != nil {
		dr.rc.Close()
		dr.rc = nil

		if errors.Is(dr.err, io.ErrUnexpectedEOF) || errors.Is(dr.err, io.EOF) {
			if dr.index != len(dr.requiredParts) {
				dr.err = nil
			}
		}
	}

	return n, dr.err
}

func (dr *dataReader) Close() error {
	if dr.rc != nil {
		err := dr.rc.Close()
		dr.rc = nil
		return err
	}

	return nil
}

func newDataReader(dataDir string, dataInfo *DataInfo, offset int64, length uint64) (*dataReader, error) {
	dataLength := int64(length)
	size := int64(dataInfo.Size)

	if offset < 0 {
		offset = size - offset
	}

	if offset < 0 {
		return nil, errors.New("insufficient data")
	}

	if offset+dataLength > size {
		return nil, errors.New("insufficient data")
	}

	requiredParts, bytesToSkip, bytesToRead := dataInfo.getParts(offset, dataLength)

	return &dataReader{
		dataDir:       dataDir,
		dataInfo:      dataInfo,
		requiredParts: requiredParts,
		bytesToSkip:   bytesToSkip,
		bytesToRead:   bytesToRead,
	}, nil
}
