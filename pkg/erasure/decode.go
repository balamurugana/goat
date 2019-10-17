package erasure

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/balamurugana/goat/pkg/boundary"
	"github.com/klauspost/reedsolomon"
)

// GetShardReader function type returns reader for shardID with limits of offset and length.
type GetShardReader func(shardID string, offset, length int64) (io.Reader, error)

type decodeReader struct {
	readersLen     uint64
	getShardReader GetShardReader
	info           *Info
	blockOffset    int64
	blockLength    int64

	readers                 []io.Reader
	shards                  [][]byte
	shardSize               uint64
	lastShardSize           uint64
	blocksToRead            uint64
	bytesToSkipInFirstBlock uint64
	bytesToReadInLastBlock  uint64
	decoder                 reedsolomon.Encoder
	errs                    []error

	index          uint64
	shardIndex     uint64
	byteIndex      uint64
	bytesAvailable uint64
}

func (dr *decodeReader) populate(count uint64, offset, length int64) error {
	for count > 0 {
		if dr.readersLen == uint64(len(dr.readers)) {
			return fmt.Errorf("too many read errors; %v", dr.errs)
		}

		var wg sync.WaitGroup
		for i := uint64(0); i < count; i++ {
			wg.Add(1)
			go func(i uint64) {
				defer wg.Done()
				dr.readers[i], dr.errs[i] = dr.getShardReader(dr.info.ShardIDs[i], offset, length)
			}(dr.readersLen + i)
		}
		wg.Wait()

		successCount := uint64(0)
		for i := uint64(0); i < count; i++ {
			if dr.errs[dr.readersLen+i] == nil {
				successCount++
			}
		}

		dr.readersLen += count
		count -= successCount
	}

	return nil
}

func (dr *decodeReader) readShards(i uint64) uint64 {
	var wg sync.WaitGroup
	for ; i < dr.readersLen; i++ {
		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()
			if dr.readers[i] != nil {
				if _, dr.errs[i] = io.ReadFull(dr.readers[i], dr.shards[i]); dr.errs[i] == nil {
					return
				}

				dr.readers[i] = nil
			}

			dr.shards[i] = dr.shards[i][:0]

			if dr.errs[i] == nil {
				dr.errs[i] = errors.New("nil reader")
			}
		}(i)
	}
	wg.Wait()

	successCount := uint64(0)
	for i := uint64(0); i < dr.readersLen; i++ {
		if dr.errs[i] == nil {
			successCount++
		}
	}

	return successCount
}

func (dr *decodeReader) readBlock() error {
	if dr.index == dr.blocksToRead {
		return io.EOF
	}

	if dr.index == 1 && dr.bytesToSkipInFirstBlock > 0 {
		for i := range dr.shards {
			dr.shards[i] = dr.shards[i][:dr.shardSize]
		}
	}

	if dr.index == dr.blocksToRead-1 && dr.shardSize != dr.lastShardSize {
		dr.shardSize = dr.lastShardSize
		for i := range dr.shards {
			dr.shards[i] = dr.shards[i][:dr.shardSize]
		}
	}

	if dr.readersLen == 0 {
		if err := dr.populate(dr.info.DataCount, dr.blockOffset, dr.blockLength); err != nil {
			return err
		}
	}

	successCount := uint64(0)
	i := uint64(0)
	for {
		if successCount += dr.readShards(i); successCount >= dr.info.DataCount {
			break
		}

		i = dr.readersLen
		if err := dr.populate(dr.info.DataCount-successCount, dr.blockOffset, dr.blockLength); err != nil {
			return err
		}
	}

	dr.blockOffset -= int64(dr.shardSize)
	dr.blockLength -= int64(dr.shardSize)

	if dr.readersLen > dr.info.DataCount {
		if err := dr.decoder.ReconstructData(dr.shards); err != nil {
			return err
		}
	}

	dr.shardIndex = 0
	dr.byteIndex = 0
	dr.bytesAvailable = dr.info.DataCount * dr.shardSize

	if dr.index == 0 {
		dr.bytesAvailable -= dr.bytesToSkipInFirstBlock
		for dr.shardIndex = 0; dr.shardIndex < dr.info.DataCount; dr.shardIndex++ {
			if dr.bytesToSkipInFirstBlock > dr.shardSize {
				dr.bytesToSkipInFirstBlock -= dr.shardSize
			} else {
				copy(dr.shards[dr.shardIndex], dr.shards[dr.shardIndex][dr.bytesToSkipInFirstBlock:])
				dr.shards[dr.shardIndex] = dr.shards[dr.shardIndex][:dr.shardSize-dr.bytesToSkipInFirstBlock]
				break
			}
		}
	}

	if dr.index == dr.blocksToRead-1 {
		dr.bytesAvailable = dr.bytesToReadInLastBlock

		var i uint64
		for i = dr.shardIndex; i < dr.info.DataCount; i++ {
			if dr.bytesToReadInLastBlock > dr.shardSize {
				dr.bytesToReadInLastBlock -= dr.shardSize
			} else {
				dr.shards[i] = dr.shards[i][:dr.bytesToReadInLastBlock]
				i++
				break
			}
		}

		for ; i < dr.info.DataCount; i++ {
			dr.shards[i] = dr.shards[i][:0]
		}
	}

	dr.index++

	return nil
}

func (dr *decodeReader) Read(b []byte) (n int, err error) {
	for n < len(b) {
		if dr.bytesAvailable == 0 {
			if err := dr.readBlock(); err != nil {
				return n, err
			}
		}

		copied := uint64(copy(b[n:], dr.shards[dr.shardIndex][dr.byteIndex:]))
		if copied < uint64(len(dr.shards[dr.shardIndex]))-dr.byteIndex {
			dr.byteIndex += copied
		} else {
			dr.shardIndex++
			dr.byteIndex = 0
		}
		dr.bytesAvailable -= copied

		n += int(copied)
	}

	return n, nil
}

// NewReader reads data shards from readers and exposes a reader.
func NewReader(getShardReader GetShardReader, shards [][]byte, info *Info, offset int64, length uint64) (io.Reader, error) {
	count := info.DataCount + info.ParityCount

	if uint64(len(shards)) != count {
		panic("len(shards) != info.DataCount+info.ParityCount")
	}

	for i, shard := range shards {
		if uint64(len(shard)) != info.ShardSize {
			panic(fmt.Errorf("len(shards[%v]) != info.ShardSize", i))
		}
	}

	if uint64(len(info.ShardIDs)) != count {
		panic("len(info.ShardIDs) != info.DataCount+info.ParityCount")
	}

	if dups := getDuplicates(info.ShardIDs); dups != nil {
		panic(fmt.Errorf("duplicate IDs %v found in info.ShardIDs", dups))
	}

	decoder, err := reedsolomon.New(int(info.DataCount), int(info.ParityCount))
	if err != nil {
		panic(err)
	}

	if offset < 0 {
		offset = int64(info.Size) - offset
	}

	if offset < 0 {
		return nil, errors.New("insufficient data")
	}

	if uint64(offset)+length > info.Size {
		return nil, errors.New("insufficient data")
	}

	blockCount, blockSize, _, lastShardSize := info.Compute()

	blocksToSkip, blocksToRead, bytesToSkipInFirstBlock, bytesToReadInLastBlock, _ := boundary.CalcBoundaries(
		int64(info.Size), int64(blockSize), int64(blockCount), offset, int64(length),
	)

	if uint64(blocksToSkip+blocksToRead) != blockCount {
		lastShardSize = info.ShardSize
	}

	return &decodeReader{
		getShardReader:          getShardReader,
		info:                    info,
		blockOffset:             blocksToSkip * int64(info.ShardSize),
		blockLength:             int64(lastShardSize) + (blocksToRead-1)*int64(info.ShardSize),
		readers:                 make([]io.Reader, count),
		shards:                  shards,
		shardSize:               info.ShardSize,
		lastShardSize:           lastShardSize,
		blocksToRead:            uint64(blocksToRead),
		bytesToSkipInFirstBlock: uint64(bytesToSkipInFirstBlock),
		bytesToReadInLastBlock:  uint64(bytesToReadInLastBlock),
		decoder:                 decoder,
		errs:                    make([]error, count),
	}, nil
}
