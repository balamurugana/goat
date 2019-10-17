package boundary

func CalcBoundaries(size, blockSize, blockCount, offset, length int64) (blocksToSkip, blocksToRead, bytesToSkipInFirstBlock, bytesToReadInLastBlock, lastBlockSize int64) {
	if offset > size || length > size || offset+length > size {
		panic("invalid offset/length for size")
	}

	lastBlockSize = size - (blockCount-1)*blockSize
	if lastBlockSize == 0 {
		lastBlockSize = blockSize
	}

	if blockCount <= 1 {
		blockSize = lastBlockSize
	}

	blocksToSkip = offset / blockSize
	bytesToSkipInFirstBlock = offset - blocksToSkip*blockSize

	if bytesToSkipInFirstBlock > 0 {
		blocksToRead++

		if length <= blockSize-bytesToSkipInFirstBlock {
			bytesToReadInLastBlock = length
			lastBlockSize = blockSize
			return
		}

		length -= (blockSize - bytesToSkipInFirstBlock)
	}

	blocksNeeded := length / blockSize
	r := length - (blocksNeeded * blockSize)
	switch {
	case r > 0:
		bytesToReadInLastBlock = r
		blocksNeeded++
	case r < 0:
		bytesToReadInLastBlock = length
	default:
		bytesToReadInLastBlock = blockSize
	}
	blocksToRead += blocksNeeded

	if blocksToSkip+blocksToRead != blockCount {
		lastBlockSize = blockSize
	}

	return
}

func CalcPartBoundaries(partSizes []int64, offset, length int64) (startPart, endPart, bytesToSkipInStartPart, bytesToReadInEndPart int64) {
	offsetFound := false
	endPart = int64(len(partSizes))

	for i, partSize := range partSizes {
		if !offsetFound {
			if partSize <= offset {
				offset -= partSize
				continue
			}

			offsetFound = true
			startPart = int64(i)
			partSize -= offset
		}

		if length <= partSize {
			endPart = int64(i) + 1
			break
		}

		length -= partSize
	}

	return startPart, endPart, offset, length
}
