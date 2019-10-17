package erasure

import (
	"errors"
	"fmt"
	"io"
	"sync"

	xhash "github.com/balamurugana/goat/pkg/hash"
	"github.com/klauspost/reedsolomon"
)

// GetShardWriter function type returns a writer for shardID.
type GetShardWriter func(shardID string) (io.Writer, error)

// Write reads block of data using shards from reader with info; then erasure encodes and writes each shards into individual writers;
// returns error if successful writer count is less than minSuccessWriters.
func Write(getShardWriter GetShardWriter, shards [][]byte, info *Info, reader io.Reader, minSuccessWriters uint64) ([]string, string, error) {
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

	encoder, err := reedsolomon.New(int(info.DataCount), int(info.ParityCount))
	if err != nil {
		panic(err)
	}

	shardSize := info.ShardSize
	blockCount, blockSize, lastBlockSize, lastShardSize := info.Compute()

	writers := make([]io.Writer, count)
	errs := make([]error, len(writers))
	var wg sync.WaitGroup
	for i := uint64(0); i < count; i++ {
		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()
			writers[i], errs[i] = getShardWriter(info.ShardIDs[i])
		}(i)
	}
	wg.Wait()

	successCount := uint64(0)
	for i := uint64(0); i < count; i++ {
		if errs[i] == nil {
			successCount++
		}
	}

	if successCount < minSuccessWriters {
		return nil, "", fmt.Errorf("multiple error on getShardWriter(); %v", errs)
	}

	clear := func(b []byte, offset, length uint64) {
		for i := offset; i < length; i++ {
			b[i] = 0
		}
	}

	dataHasher := xhash.MustGetNewHash(xhash.HighwayHash256Algorithm, nil)
	readDataShards := func() error {
		bytesRead := uint64(0)
		for s := uint64(0); s < info.DataCount; s++ {
			n, err := io.ReadFull(reader, shards[s])
			bytesRead += uint64(n)
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) && bytesRead == blockSize {
					err = nil
					dataHasher.Write(shards[s][:n])
					clear(shards[s], uint64(n), shardSize)
					for s++; s < info.DataCount; s++ {
						clear(shards[s], 0, shardSize)
					}
				}

				return err
			}

			dataHasher.Write(shards[s])
		}

		return nil
	}

	shardHashers := make([]xhash.Hash, count)
	for i := range shardHashers {
		shardHashers[i] = xhash.MustGetNewHash(xhash.HighwayHash256Algorithm, nil)
	}
	writeShards := func() error {
		var wg sync.WaitGroup
		for i := 0; i < len(writers); i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if writers[i] != nil {
					if _, errs[i] = writers[i].Write(shards[i]); errs[i] == nil {
						shardHashers[i].Write(shards[i])
						return
					}

					writers[i] = nil
					shardHashers[i] = nil
				}

				if errs[i] == nil {
					errs[i] = errors.New("nil writer")
				}
			}(i)
		}
		wg.Wait()

		successCount := uint64(0)
		for i := 0; i < len(errs); i++ {
			if errs[i] == nil {
				successCount++
			}
		}

		if successCount < minSuccessWriters {
			return fmt.Errorf("too many write errors; %v", errs)
		}

		return nil
	}

	for i := uint64(0); i < blockCount; i++ {
		if i == blockCount-1 {
			blockSize = lastBlockSize

			if shardSize != lastShardSize {
				shardSize = lastShardSize
				for s := range shards {
					shards[s] = shards[s][:shardSize]
				}
			}
		}

		if err := readDataShards(); err != nil {
			return nil, "", err
		}

		if err := encoder.Encode(shards); err != nil {
			return nil, "", err
		}

		ok, err := encoder.Verify(shards)
		if err != nil {
			return nil, "", err
		}
		if !ok {
			return nil, "", fmt.Errorf("verification failed on encoded shards")
		}

		if err := writeShards(); err != nil {
			return nil, "", err
		}
	}

	shardSums := make([]string, count)
	for i := range shardHashers {
		if shardHashers[i] != nil {
			shardSums[i] = shardHashers[i].HexSum(nil)
		}
	}

	return shardSums, dataHasher.HexSum(nil), nil
}
