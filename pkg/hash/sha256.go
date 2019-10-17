package hash

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
)

// SHA256 is SHA-256 hash algorithm.
type SHA256 struct {
	hash.Hash
}

// NewSHA256 creates new SHA-256 hash.
func NewSHA256() *SHA256 {
	return &SHA256{sha256.New()}
}

// Name returns SHA256Algorithm.
func (hasher *SHA256) Name() string {
	return SHA256Algorithm
}

// HashKey is available for interface compatibility.
func (hasher *SHA256) HashKey() string {
	return ""
}

// HashLength returns length of returning string of HexSum().
func (hasher *SHA256) HashLength() uint {
	return 64
}

// HexSum is same as Sum() but returns hash in hex encoded string.
func (hasher *SHA256) HexSum(data []byte) string {
	return hex.EncodeToString(hasher.Sum(data))
}
