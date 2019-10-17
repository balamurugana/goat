package erasure

func getDuplicates(sl []string) (dups []string) {
	m := map[string]struct{}{}
	for _, s := range sl {
		if _, found := m[s]; found {
			dups = append(dups, s)
		} else {
			m[s] = struct{}{}
		}
	}

	return dups
}

type Info struct {
	DataCount   uint64 `json:"dataCount"`
	ParityCount uint64 `json:"parityCount"`
	Size        uint64 `json:"size"`
	ShardSize   uint64 `json:"shardSize"`

	// must be len(ShardIDs) == DataCount+ParityCount and each ID should be unique each other and must be populated by data count then parity count.
	// e.g.
	// DataCount, ParityCount := 2, 3
	// ShardIDs := make([]string, DataCount+ParityCount)
	// for i := 0; i < DataCount; i++ {
	//     ShardIDs = fmt.Sprintf("shard.%v", i)
	// }
	// for i := 0; i < ParityCount; i++ {
	//     ShardIDs = fmt.Sprintf("shard.%v", DataCount+i)
	// }
	ShardIDs []string `json:"shardIDs"`
}

func (info Info) Compute() (blockCount, blockSize, lastBlockSize, lastShardSize uint64) {
	blockSize = info.DataCount * info.ShardSize
	blockCount = info.Size / blockSize
	lastBlockSize = info.Size - blockCount*blockSize
	if lastBlockSize > 0 {
		blockCount++
		lastShardSize = (lastBlockSize + info.DataCount - 1) / info.DataCount
	} else {
		lastBlockSize = blockSize
		lastShardSize = info.ShardSize
	}

	return blockCount, blockSize, lastBlockSize, lastShardSize
}
