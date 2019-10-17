package hash

import (
	"crypto/md5"
	"encoding/hex"
	"hash"
)

// MD5 is MD5 hash algorithm.
type MD5 struct {
	hash.Hash
}

// NewMD5 creates new MD5 hash.
func NewMD5() *MD5 {
	return &MD5{md5.New()}
}

// Name returns MD5Algorithm.
func (hasher *MD5) Name() string {
	return MD5Algorithm
}

// HashKey is available for interface compatibility.
func (hasher *MD5) HashKey() string {
	return ""
}

// HashLength returns length of returning string of HexSum().
func (hasher *MD5) HashLength() uint {
	return 32
}

// HexSum is same as Sum() but returns hash in hex encoded string.
func (hasher *MD5) HexSum(data []byte) string {
	return hex.EncodeToString(hasher.Sum(data))
}
