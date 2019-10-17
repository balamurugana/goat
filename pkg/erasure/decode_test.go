package erasure

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"

	xhash "github.com/balamurugana/goat/pkg/hash"
	xos "github.com/balamurugana/goat/pkg/os"
	xrand "github.com/balamurugana/goat/pkg/rand"
)

func TestNewReader(t *testing.T) {
	testCases := []struct {
		info     *Info
		offset   int64
		length   uint64
		checksum string
	}{
		{
			info: &Info{
				DataCount:   1,
				ParityCount: 3,
				Size:        32283,
				ShardSize:   MiB,
			},
			offset:   0,
			length:   10,
			checksum: "cb681256c303aaacfc24ed94cb5ffd6a84fcde8a6721213b0a757ba40ac4a4a9",
		},
		{
			info: &Info{
				DataCount:   1,
				ParityCount: 3,
				Size:        32283,
				ShardSize:   MiB,
			},
			offset:   10,
			length:   7,
			checksum: "aa88fcc3f216be54199c57fd835b9921a6fd259edc834d115b6b898ccfaa4c25",
		},
		{
			info: &Info{
				DataCount:   1,
				ParityCount: 3,
				Size:        70009289,
				ShardSize:   MiB,
			},
			offset:   3145649,
			length:   1048986,
			checksum: "3faf5850c140d6f2ad36e0ba7324d306e1589d50fc17fa0cc1a1ccbf76d87332",
		},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				dirname := xrand.NewID(8).String()
				defer os.RemoveAll(dirname)
				testWrite(t, testCase.info, dirname)

				files := map[string]*os.File{}
				filesMutex := sync.Mutex{}
				getShardReader := func(shardID string, offset, length int64) (io.Reader, error) {
					file, err := os.Open(shardID)
					if err != nil {
						return nil, err
					}

					filesMutex.Lock()
					files[shardID] = file
					filesMutex.Unlock()
					return xos.NewSectionFileReader(file, offset, length), nil
				}

				length := testCase.info.DataCount + testCase.info.ParityCount
				shards := make([][]byte, length)
				for j := uint64(0); j < length; j++ {
					shards[j] = make([]byte, testCase.info.ShardSize)
				}

				defer func() {
					for _, file := range files {
						file.Close()
					}
				}()

				reader, err := NewReader(getShardReader, shards, testCase.info, testCase.offset, testCase.length)
				if err != nil {
					t.Fatal(err)
				}

				hasher := xhash.MustGetNewHash(xhash.HighwayHash256Algorithm, nil)
				if _, err = io.Copy(hasher, reader); err != nil {
					t.Fatal(err)
				}
				checksum := hasher.HexSum(nil)
				if !reflect.DeepEqual(checksum, testCase.checksum) {
					t.Fatalf("expected: %v, got: %v", testCase.checksum, checksum)
				}
			},
		)
	}
}
