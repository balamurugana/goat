package erasure

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/balamurugana/goat/pkg/boundary"
	"github.com/balamurugana/goat/pkg/erasure"
)

type dataReader struct {
	getShardReader func(shardID string, offset, length int64) (io.ReadCloser, error)
	shardPartsSize uint64
	parts          []Part
	index          int
	bytesToSkip    int64
	bytesToRead    int64

	rcsMap      map[string]io.ReadCloser
	rcsMapMutex sync.Mutex
	reader      io.Reader
	shards      [][]byte

	err error
}

func (dr *dataReader) closeShardReaders() error {
	errs := make([]error, len(dr.rcsMap))
	var i int

	var wg sync.WaitGroup
	dr.rcsMapMutex.Lock()
	for id, rc := range dr.rcsMap {
		if rc != nil {
			wg.Add(1)
			go func(i int, id string, rc io.ReadCloser) {
				defer wg.Done()
				if errs[i] = rc.Close(); errs[i] != nil {
					errs[i] = fmt.Errorf("%v: %v", id, errs[i])
				}
			}(i, id, rc)
		}
		delete(dr.rcsMap, id)
		i++
	}
	dr.rcsMapMutex.Unlock()
	wg.Wait()

	var errsOnly []error
	for _, err := range errs {
		if err != nil {
			errsOnly = append(errsOnly, err)
		}
	}

	if len(errsOnly) > 0 {
		return fmt.Errorf("multiple close error; %v", errsOnly)
	}

	return nil
}

func (dr *dataReader) getReader(offset int64, length uint64) (io.Reader, error) {
	dr.closeShardReaders()

	shardPartsSize := dr.shardPartsSize
	getShardReader := func(shardID string, offset, length int64) (io.Reader, error) {
		rc, err := dr.getShardReader(shardID, int64(shardPartsSize)+offset, length)
		if err == nil {
			dr.rcsMapMutex.Lock()
			dr.rcsMap[shardID] = rc
			dr.rcsMapMutex.Unlock()
		}
		return rc, err
	}

	for i := range dr.shards {
		dr.shards[i] = dr.shards[i][:cap(dr.shards[i])]
	}

	return erasure.NewReader(getShardReader, dr.shards, &dr.parts[dr.index].Info, offset, length)
}

func (dr *dataReader) Read(b []byte) (int, error) {
	if dr.err != nil {
		if errors.Is(dr.err, io.ErrUnexpectedEOF) {
			if dr.index == len(dr.parts) {
				dr.err = io.EOF
			}
		}

		return 0, dr.err
	}

	if dr.reader == nil {
		offset := int64(0)
		if dr.index == 0 {
			offset = dr.bytesToSkip

			dr.shards = make([][]byte, dr.parts[dr.index].DataCount+dr.parts[dr.index].ParityCount)
			for i := range dr.shards {
				dr.shards[i] = make([]byte, dr.parts[dr.index].ShardSize)
			}
		}

		length := dr.parts[dr.index].Size - uint64(offset)
		if dr.index == len(dr.parts)-1 {
			length = uint64(dr.bytesToRead)
		}

		if dr.reader, dr.err = dr.getReader(offset, length); dr.err != nil {
			return 0, dr.err
		}

		blockCount, _, _, lastShardSize := dr.parts[dr.index].Compute()
		dr.shardPartsSize += lastShardSize + (blockCount-1)*dr.parts[dr.index].ShardSize

		dr.index++
	}

	var n int

	if n, dr.err = dr.reader.Read(b); dr.err != nil {
		dr.closeShardReaders()
		dr.reader = nil

		if errors.Is(dr.err, io.ErrUnexpectedEOF) || errors.Is(dr.err, io.EOF) {
			if dr.index != len(dr.parts) {
				dr.err = nil
			}
		}
	}

	return n, dr.err
}

func (dr *dataReader) Close() error {
	if dr.reader != nil {
		err := dr.closeShardReaders()
		dr.reader = nil
		return err
	}

	return nil
}

func newDataReader(getShardReader func(shardID string, offset, length int64) (io.ReadCloser, error), dataInfo *DataInfo, offset int64, length uint64) (*dataReader, error) {
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

	partSizes := make([]int64, len(dataInfo.Parts))
	for i, part := range dataInfo.Parts {
		partSizes[i] = int64(part.Size)
	}
	startPart, endPart, bytesToSkip, bytesToRead := boundary.CalcPartBoundaries(partSizes, offset, dataLength)

	shardPartsSize := uint64(0)
	for i := int64(0); i < startPart; i++ {
		blockCount, _, _, lastShardSize := dataInfo.Parts[i].Compute()
		shardPartsSize += lastShardSize + (blockCount-1)*dataInfo.Parts[i].ShardSize
	}

	return &dataReader{
		getShardReader: getShardReader,
		shardPartsSize: shardPartsSize,
		parts:          dataInfo.Parts[startPart:endPart],
		bytesToSkip:    bytesToSkip,
		bytesToRead:    bytesToRead,
		rcsMap:         make(map[string]io.ReadCloser),
	}, nil
}
