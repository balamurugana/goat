package rand

import (
	crand "crypto/rand"
	"math"
	"math/big"
	"math/rand"
)

var random *rand.Rand

func init() {
	n, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(err)
	}

	random = rand.New(rand.NewSource(n.Int64()))
}

// Perm returns, as a slice of n ints, a pseudo-random permutation of the integers [0,n) from the default random source.
func Perm(n int) []int {
	return random.Perm(n)
}
