package time

import (
	"crypto/rand"
	"math/big"
	"time"
)

// RandTicker - holds a channel that delivers `ticks' of a clock at random intervals
// between minimum and maximum duration.
type RandTicker struct {
	timer  *time.Timer
	doneCh chan struct{}
	C      <-chan time.Time
}

// Stop - turns off a ticker. After Stop, no more ticks will be sent. Stop closes the
// channel hence a read from the channel needs to check whether the channel is closed
// or not.
func (ticker *RandTicker) Stop() {
	if ticker.timer != nil {
		ticker.timer.Stop()
	}

	if ticker.doneCh != nil {
		close(ticker.doneCh)
	}
}

// NewRandTicker - returns a new RandTicker containing a channel that
// will send `ticks' of a clock at random intervals between minimum and
// maximum duration.  min and max durations must be greater than zero and
// max duration must not be less than min duration; if not, NewRandTicker
// will panic.  Stop the ticker to release associated resources.
func NewRandTicker(min, max time.Duration) *RandTicker {
	if min <= time.Duration(0) {
		panic("negative min duration")
	}

	if max <= time.Duration(0) {
		panic("negative max duration")
	}

	if min > max {
		panic("min duration must be less than max duration")
	}

	getRandomDuration := func() time.Duration {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
		if err != nil {
			panic(err)
		}

		d := time.Duration(n.Int64())
		if d < min {
			if d = max - d; d < min {
				d = min
			}
		}

		return d
	}

	timer := time.NewTimer(getRandomDuration())
	timeCh := make(chan time.Time)
	doneCh := make(chan struct{})

	go func() {
		// Read from timer and return time/status. If doneCh is closed when
		// reading, it returns read failure.
		read := func() (t time.Time, ok bool) {
			select {
			case t, ok = <-timer.C:
				return t, ok
			case <-doneCh:
				return t, false
			}
		}

		// Write given time to timeCh and return status. If doneCh is closed when
		// writing, it returns write failure.
		send := func(t time.Time) bool {
			select {
			case timeCh <- t:
				return true
			case <-doneCh:
				return false
			}
		}

		for {
			t, ok := read()
			if !ok {
				break
			}

			if !send(t) {
				break
			}

			timer.Reset(getRandomDuration())
		}

		close(timeCh)
	}()

	return &RandTicker{
		timer:  timer,
		doneCh: doneCh,
		C:      timeCh,
	}
}
