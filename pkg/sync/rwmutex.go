package sync

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	xtime "github.com/balamurugana/goat/pkg/time"
)

var (
	// ErrTimedOut denotes timed out error.
	ErrTimedOut = errors.New("timed out")

	// ErrCancelled denotes cancelled error.
	ErrCancelled = errors.New("cancelled")
)

const (
	writeLock = -1
	noLock    = 0
)

// RWMutex is read/write mutex with timeout.
type RWMutex struct {
	mutex sync.Mutex
	state int64
	ctx   context.Context
}

// NewRWMutex creates new RWMutex.
func NewRWMutex(ctx context.Context) *RWMutex {
	return &RWMutex{ctx: ctx}
}

func (mu *RWMutex) setLock(isWriteLock bool) bool {
	mu.mutex.Lock()
	defer mu.mutex.Unlock()

	if isWriteLock {
		if mu.state == noLock {
			mu.state = writeLock
			return true
		}
	} else {
		if mu.state != writeLock {
			mu.state++
			return true
		}
	}

	return false
}

func (mu *RWMutex) lock(timeout time.Duration, isWriteLock bool) error {
	if timeout == time.Duration(0) {
		timeout = time.Duration(math.MaxInt64)
	}

	var ticker *time.Ticker
	var expTicker *xtime.ExpTicker

	for !mu.setLock(isWriteLock) {
		if expTicker == nil {
			expTicker = xtime.NewExpTicker(time.Millisecond, timeout)
			defer expTicker.Stop()
		}

		if ticker == nil {
			ticker = time.NewTicker(timeout)
			defer ticker.Stop()
		}

		if mu.ctx != nil {
			select {
			case <-mu.ctx.Done():
				return ErrCancelled
			case <-ticker.C:
				return ErrTimedOut
			case <-expTicker.C:
			}
		} else {
			select {
			case <-ticker.C:
				return ErrTimedOut
			case <-expTicker.C:
			}
		}
	}

	return nil
}

func (mu *RWMutex) unlock(isWriteLock bool) bool {
	mu.mutex.Lock()
	defer mu.mutex.Unlock()

	if isWriteLock {
		if mu.state == writeLock {
			mu.state = noLock
			return true
		}
	} else {
		if mu.state != noLock && mu.state != writeLock {
			mu.state--
			return true
		}
	}

	return false
}

// Lock does write lock within given timeout.
func (mu *RWMutex) Lock(timeout time.Duration) error {
	return mu.lock(timeout, true)
}

// RLock does read lock within given timeout.
func (mu *RWMutex) RLock(timeout time.Duration) error {
	return mu.lock(timeout, false)
}

// Unlock does write unlock.
func (mu *RWMutex) Unlock() bool {
	return mu.unlock(true)
}

// RUnlock does read unlock.
func (mu *RWMutex) RUnlock() bool {
	return mu.unlock(false)
}
