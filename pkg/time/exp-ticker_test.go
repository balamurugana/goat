package time

import (
	"testing"
	"time"
)

func TestExpTicker(t *testing.T) {
	testCases := []struct {
		min time.Duration
		max time.Duration
	}{
		{10 * time.Millisecond, 1 * time.Second},
		// ExpTicker works exactly like time.Ticker here, but not recommended for practical use.
		{10 * time.Millisecond, 10 * time.Millisecond},
	}

	for i, testCase := range testCases {
		count := 10
		minDelta := testCase.min * time.Duration(count)
		maxDelta := testCase.max * time.Duration(count)
		maxSlop := 2 * testCase.max

		ticker := NewExpTicker(testCase.min, testCase.max)
		t1 := time.Now()
		for j := 0; j < count; j++ {
			<-ticker.C
		}
		ticker.Stop()
		delta := time.Now().Sub(t1)

		if (delta < minDelta) || (delta > maxDelta+maxSlop) {
			t.Fatalf("case %v: got: %v, expected: between %v and %v", i+1, delta, minDelta, maxDelta+maxSlop)
		}
	}
}

func TestExpTickerStopWithDirectInitialization(t *testing.T) {
	c := make(chan time.Time)
	ticker := &ExpTicker{C: c}
	ticker.Stop()
}

func TestNewExpTickerPanics(t *testing.T) {
	testCases := []struct {
		min time.Duration
		max time.Duration
	}{
		{-1, 10},
		{10, 0},
		{10, 9},
	}

	for i, testCase := range testCases {
		func() {
			defer func() {
				if err := recover(); err == nil {
					t.Fatalf("case %v: NewExpTicker(%v, %v) should have panicked", i+1, testCase.min, testCase.max)
				}
			}()
			NewExpTicker(testCase.min, testCase.max)
		}()
	}
}
