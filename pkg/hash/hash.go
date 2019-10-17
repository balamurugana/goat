package hash

import (
	"encoding/base64"
	"errors"
	"hash"
)

// ErrUnknownAlgorithm denotes unknown algorithm error.
var ErrUnknownAlgorithm = errors.New("unknown algorithm")

// Hash is an extended interface of hash.Hash in standard library.
type Hash interface {
	hash.Hash
	Name() string
	HashKey() string
	HashLength() uint
	HexSum([]byte) string // Same as Sum() but returns hash in hex encoded string.
}

const (
	// HighwayHash256Algorithm denotes highway-256 hash algorithm.
	HighwayHash256Algorithm = "HighwayHash256"

	// SHA256Algorithm denotes SHA-256 hash algorithm.
	SHA256Algorithm = "SHA256"

	// MD5Algorithm denotes MD5 hash algorithm.
	MD5Algorithm = "MD5"
)

// NewHash creates new hash for given name and key.
func NewHash(name string, key []byte) (Hash, error) {
	switch name {
	case HighwayHash256Algorithm:
		return NewHighwayHash256(key)
	case SHA256Algorithm:
		return NewSHA256(), nil
	case MD5Algorithm:
		return NewMD5(), nil
	}

	return nil, ErrUnknownAlgorithm
}

// MustGetNewHash creates new hash for given name and key; panics on error.
func MustGetNewHash(name string, key []byte) Hash {
	hash, err := NewHash(name, key)
	if err != nil {
		panic(err)
	}

	return hash
}

// SumInBase64 returns hash string in base-64 of given strings.
func SumInBase64(s ...string) string {
	hasher := MustGetNewHash(HighwayHash256Algorithm, nil)
	for i := range s {
		hasher.Write([]byte(s[i]))
	}

	return base64.RawURLEncoding.EncodeToString(hasher.Sum(nil))
}
