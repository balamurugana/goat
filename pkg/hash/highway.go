package hash

import (
	"encoding/hex"
	"hash"

	"github.com/minio/highwayhash"
)

var defaultHighwayHashKey = []byte{
	1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 13, 14, 15, 16,
	17, 18, 19, 20, 21, 22, 23, 24,
	25, 26, 27, 28, 29, 30, 31, 32,
}

// HighwayHash256 is highway-256 hash algorithm.
type HighwayHash256 struct {
	hash.Hash
	key string
}

// NewHighwayHash256 creates new highway hash with given key.
func NewHighwayHash256(key []byte) (*HighwayHash256, error) {
	if key == nil {
		key = defaultHighwayHashKey
	}

	hasher, err := highwayhash.New(key)
	if err != nil {
		return nil, err
	}

	return &HighwayHash256{
		Hash: hasher,
		key:  hex.EncodeToString(key),
	}, nil
}

// Name returns HighwayHash256Algorithm.
func (hasher *HighwayHash256) Name() string {
	return HighwayHash256Algorithm
}

// HashKey returns key as hex encoded string.
func (hasher *HighwayHash256) HashKey() string {
	return hasher.key
}

// HashLength returns length of returning string of HexSum().
func (hasher *HighwayHash256) HashLength() uint {
	return 64
}

// HexSum is same as Sum() but returns hash in hex encoded string.
func (hasher *HighwayHash256) HexSum(data []byte) string {
	return hex.EncodeToString(hasher.Sum(data))
}
