package erasure

import (
	"fmt"
	"testing"
)

func TestInfoCompute(t *testing.T) {
	testCases := []struct {
		info          Info
		blockCount    uint64
		blockSize     uint64
		lastBlockSize uint64
		lastShardSize uint64
	}{
		{Info{1, 3, 32283, MiB, nil}, 1, 1 * MiB, 32283, 32283},
		{Info{4, 4, 32283, MiB, nil}, 1, 4 * MiB, 32283, 8071},
		{Info{4, 2, 32283, MiB, nil}, 1, 4 * MiB, 32283, 8071},
		{Info{4, 7, 32283, MiB, nil}, 1, 4 * MiB, 32283, 8071},
		{Info{4, 4, MiB, MiB, nil}, 1, 4 * MiB, MiB, MiB / 4},
		{Info{4, 4, 4 * MiB, MiB, nil}, 1, 4 * MiB, 4 * MiB, MiB},
		{Info{4, 4, 2 * 4 * MiB, MiB, nil}, 2, 4 * MiB, 4 * MiB, MiB},
		{Info{4, 4, 32283 + MiB, MiB, nil}, 1, 4 * MiB, 32283 + MiB, 270215},
		{Info{4, 4, 32283 + 4*MiB, MiB, nil}, 2, 4 * MiB, 32283, 8071},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				blockCount, blockSize, lastBlockSize, lastShardSize := testCase.info.Compute()

				if blockCount != testCase.blockCount {
					t.Fatalf("expected: %v, got: %v", testCase.blockCount, blockCount)
				}

				if blockSize != testCase.blockSize {
					t.Fatalf("expected: %v, got: %v", testCase.blockSize, blockSize)
				}

				if lastBlockSize != testCase.lastBlockSize {
					t.Fatalf("expected: %v, got: %v", testCase.lastBlockSize, lastBlockSize)
				}

				if lastShardSize != testCase.lastShardSize {
					t.Fatalf("expected: %v, got: %v", testCase.lastShardSize, lastShardSize)
				}
			},
		)
	}
}
