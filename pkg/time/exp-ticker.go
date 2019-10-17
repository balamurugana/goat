package time

import (
	"math"
	"time"
)

// ExpTicker - holds a channel that delivers `ticks' of a clock at exponential
// interval of minimum duration but not greater than maximum duration.
type ExpTicker struct {
	timer  *time.Timer
	doneCh chan struct{}
	C      <-chan time.Time
}

// Stop - turns off a ticker. After Stop, no more ticks will be sent. Stop closes the
// channel hence a read from the channel needs to check whether the channel is closed
// or not.
func (ticker *ExpTicker) Stop() {
	if ticker.timer != nil {
		ticker.timer.Stop()
	}

	if ticker.doneCh != nil {
		close(ticker.doneCh)
	}
}

// NewExpTicker - returns a new ExpTicker containing a channel that
// will send `ticks' of a clock at exponential interval of minimum duration
// (min * 2 ** x) but not greater than maximum duration.  min and max
// durations must be greater than zero and max duration must not be less than
// min duration; if not, NewExpTicker will panic.
// Stop the ticker to release associated resources.
func NewExpTicker(min, max time.Duration) *ExpTicker {
	if min <= time.Duration(0) {
		panic("negative min duration")
	}

	if max <= time.Duration(0) {
		panic("negative max duration")
	}

	if min > max {
		panic("min duration must be less than max duration")
	}

	var exponent float64
	getExponentDuration := func() time.Duration {
		d := min * time.Duration(math.Exp2(exponent))
		if d > max {
			d = min
			exponent = 0.0
		}
		exponent += 1.0

		return d
	}

	timer := time.NewTimer(getExponentDuration())
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

			timer.Reset(getExponentDuration())
		}

		close(timeCh)
	}()

	return &ExpTicker{
		timer:  timer,
		doneCh: doneCh,
		C:      timeCh,
	}
}
