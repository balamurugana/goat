package boundary

import (
	"fmt"
	"testing"
)

const MiB = 1 * 1024 * 1024 // 1 MiB.

func TestCalcBoundaries(t *testing.T) {
	testCases := []struct {
		size       int64
		blockSize  int64
		blockCount int64

		// args
		offset int64
		length int64

		// output
		blocksToSkip            int64
		blocksToRead            int64
		bytesToSkipInFirstBlock int64
		bytesToReadInLastBlock  int64
		lastBlockSize           int64
	}{
		{
			16279, MiB, 1,
			0, 10,
			0, 1, 0, 10, 16279,
		},
		{
			16279, MiB, 1,
			10, 7,
			0, 1, 10, 7, 16279,
		},
		{
			16279, MiB, 1,
			12958, 3321,
			0, 1, 12958, 3321, 16279,
		},
		{
			16279, MiB, 1,
			0, 16279,
			0, 1, 0, 16279, 16279,
		},
		{
			70009289, MiB, 67,
			0, 10,
			0, 1, 0, 10, MiB,
		},
		{
			70009289, MiB, 67,
			10, 7,
			0, 1, 10, 7, MiB,
		},
		{
			70009289, MiB, 67,
			0, MiB,
			0, 1, 0, MiB, MiB,
		},
		{
			70009289, MiB, 67,
			10, MiB,
			0, 2, 10, 10, MiB,
		},
		{
			70009289, MiB, 67,
			3145649, 1048986,
			2, 3, 1048497, 331, MiB,
		},
		{
			70009289, MiB, 67,
			69206016, 803273,
			66, 1, 0, 803273, 803273,
		},
		{
			70009289, MiB, 67,
			69205916, 803273,
			65, 2, 1048476, 803173, 803273,
		},
		{
			70009289, MiB, 67,
			69206016, 100,
			66, 1, 0, 100, 803273,
		},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				blocksToSkip, blocksToRead, bytesToSkipInFirstBlock, bytesToReadInLastBlock, lastBlockSize := CalcBoundaries(
					testCase.size, testCase.blockSize, testCase.blockCount, testCase.offset, testCase.length,
				)

				if testCase.blocksToSkip != blocksToSkip {
					t.Fatalf("mismatch: blocksToSkip: expected: %v, got: %v", testCase.blocksToSkip, blocksToSkip)
				}

				if testCase.blocksToRead != blocksToRead {
					t.Fatalf("mismatch: blocksToRead: expected: %v, got: %v", testCase.blocksToRead, blocksToRead)
				}

				if testCase.bytesToSkipInFirstBlock != bytesToSkipInFirstBlock {
					t.Fatalf("mismatch: bytesToSkipInFirstBlock: expected: %v, got: %v", testCase.bytesToSkipInFirstBlock, bytesToSkipInFirstBlock)
				}

				if testCase.bytesToReadInLastBlock != bytesToReadInLastBlock {
					t.Fatalf("mismatch: bytesToReadInLastBlock: expected: %v, got: %v", testCase.bytesToReadInLastBlock, bytesToReadInLastBlock)
				}

				if testCase.lastBlockSize != lastBlockSize {
					t.Fatalf("mismatch: lastBlockSize: expected: %v, got: %v", testCase.lastBlockSize, lastBlockSize)
				}
			},
		)
	}
}

func TestCalcPartBoundaries(t *testing.T) {
	testCases := []struct {
		partSizes              []int64
		offset                 int64
		length                 int64
		startPart              int64
		endPart                int64
		bytesToSkipInStartPart int64
		bytesToReadInEndPart   int64
	}{
		{[]int64{16279}, 0, 10, 0, 1, 0, 10},
		{[]int64{16279, 10992}, 0, 10, 0, 1, 0, 10},
		{[]int64{16279}, 10, 7, 0, 1, 10, 7},
		{[]int64{16279, 10992}, 10, 7, 0, 1, 10, 7},
		{[]int64{16279}, 0, 16279, 0, 1, 0, 16279},
		{[]int64{16279, 10992}, 16279, 10992, 1, 2, 0, 10992},
		{[]int64{16279, 10992}, 12958, 10992, 0, 2, 12958, 7671},
		{[]int64{16279, 10992, 25489}, 12958, 17343, 0, 3, 12958, 3030},
		{[]int64{16279, 10992, 25489}, 27271, 70, 2, 3, 0, 70},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				startPart, endPart, bytesToSkipInStartPart, bytesToReadInEndPart := CalcPartBoundaries(
					testCase.partSizes, testCase.offset, testCase.length,
				)

				if testCase.startPart != startPart {
					t.Fatalf("mismatch: startPart: expected: %v, got: %v", testCase.startPart, startPart)
				}

				if testCase.endPart != endPart {
					t.Fatalf("mismatch: endPart: expected: %v, got: %v", testCase.endPart, endPart)
				}

				if testCase.bytesToSkipInStartPart != bytesToSkipInStartPart {
					t.Fatalf("mismatch: bytesToSkipInStartPart: expected: %v, got: %v", testCase.bytesToSkipInStartPart, bytesToSkipInStartPart)
				}

				if testCase.bytesToReadInEndPart != bytesToReadInEndPart {
					t.Fatalf("mismatch: bytesToReadInLastBlock: expected: %v, got: %v", testCase.bytesToReadInEndPart, bytesToReadInEndPart)
				}
			},
		)
	}
}
