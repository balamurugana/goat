package sync

import (
	"sync"
	"time"
)

type nameMutex struct {
	RWMutex
	counter int64
}

// NameMutex is read/write mutex of names.
type NameMutex struct {
	sync.Mutex
	lockMap map[string]*nameMutex
}

// NewNameMutex creates new NameMutex.
func NewNameMutex() *NameMutex {
	return &NameMutex{
		lockMap: make(map[string]*nameMutex),
	}
}

func (locker *NameMutex) lock(name string, timeout time.Duration, isWriteLock bool) (err error) {
	locker.Mutex.Lock()

	mutex, found := locker.lockMap[name]
	if !found {
		mutex = new(nameMutex)

		// Ensure any call to this name gets same mutex.
		locker.lockMap[name] = mutex
	}

	locker.Mutex.Unlock()

	if isWriteLock {
		err = mutex.Lock(timeout)
	} else {
		err = mutex.RLock(timeout)
	}
	if err != nil {
		locker.Mutex.Lock()
		if mutex.counter <= 0 {
			delete(locker.lockMap, name)
		}
		locker.Mutex.Unlock()

		return err
	}

	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	mutex.counter++

	// A parallel unlock() could have removed this. Make sure its not lost.
	locker.lockMap[name] = mutex

	return nil
}

func (locker *NameMutex) unlock(name string, isWriteLock bool) error {
	locker.Mutex.Lock()
	defer locker.Mutex.Unlock()

	if mutex, found := locker.lockMap[name]; found {
		if isWriteLock {
			mutex.Unlock()
		} else {
			mutex.RUnlock()
		}
		mutex.counter--
		if mutex.counter <= 0 {
			delete(locker.lockMap, name)
		}
	}

	return nil
}

// Lock does write lock given name within given timeout.
func (locker *NameMutex) Lock(name string, timeout time.Duration) (err error) {
	return locker.lock(name, timeout, true)
}

// RLock does read lock given name within given timeout.
func (locker *NameMutex) RLock(name string, timeout time.Duration) error {
	return locker.lock(name, timeout, false)
}

// Unlock does write unlock given name.
func (locker *NameMutex) Unlock(name string) error {
	return locker.unlock(name, true)
}

// RUnlock does read unlock given name.
func (locker *NameMutex) RUnlock(name string) error {
	return locker.unlock(name, false)
}
