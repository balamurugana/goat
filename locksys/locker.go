package locksys

import (
	"errors"
	"log"
	"sync"
	"time"
)

type Locker interface {
	Lock(name string, timeout time.Duration) error
	RLock(name string, timeout time.Duration) error
	Unlock(name string) error
	RUnlock(name string) error
}

var (
	errNilLocker   = errors.New("nil locker")
	errReadQuorum  = errors.New("read quorum error")
	errWriteQuorum = errors.New("write quorum error")
)

func getErrCount(errs []error, err error) int {
	counter := 0
	for i := range errs {
		if errs[i] == err {
			counter++
		}
	}

	return counter
}

type lockerList struct {
	lockers     []Locker
	readQuorum  int
	writeQuorum int
}

func newLockerList(lockers []Locker, readQuorum, writeQuorum int) *lockerList {
	return &lockerList{
		lockers:     lockers,
		readQuorum:  readQuorum,
		writeQuorum: writeQuorum,
	}
}

func (lockers *lockerList) Lock(name string, timeout time.Duration) error {
	errs := make([]error, len(lockers.lockers))
	var wg sync.WaitGroup
	for i := range lockers.lockers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if lockers.lockers[i] == nil {
				errs[i] = errNilLocker
				return
			}

			if errs[i] = lockers.lockers[i].Lock(name, timeout); errs[i] != nil {
				lockers.lockers[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) >= lockers.writeQuorum {
		return nil
	}

	log.Println("lockerList.Lock() failed.", errs)
	return errWriteQuorum
}

func (lockers *lockerList) RLock(name string, timeout time.Duration) error {
	errs := make([]error, len(lockers.lockers))
	var wg sync.WaitGroup
	for i := range lockers.lockers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if lockers.lockers[i] == nil {
				errs[i] = errNilLocker
				return
			}

			if errs[i] = lockers.lockers[i].RLock(name, timeout); errs[i] != nil {
				lockers.lockers[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) >= lockers.readQuorum {
		return nil
	}

	log.Println("lockerList.RLock() failed.", errs)
	return errReadQuorum
}

func (lockers *lockerList) Unlock(name string) error {
	errs := make([]error, len(lockers.lockers))
	var wg sync.WaitGroup
	for i := range lockers.lockers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if lockers.lockers[i] == nil {
				errs[i] = errNilLocker
				return
			}

			if errs[i] = lockers.lockers[i].Unlock(name); errs[i] != nil {
				lockers.lockers[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) >= lockers.writeQuorum {
		return nil
	}

	log.Println("lockerList.Unlock() failed.", errs)
	return errWriteQuorum
}

func (lockers *lockerList) RUnlock(name string) error {
	errs := make([]error, len(lockers.lockers))
	var wg sync.WaitGroup
	for i := range lockers.lockers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if lockers.lockers[i] == nil {
				errs[i] = errNilLocker
				return
			}

			if errs[i] = lockers.lockers[i].RUnlock(name); errs[i] != nil {
				lockers.lockers[i] = nil
			}
		}(i)
	}
	wg.Wait()

	if getErrCount(errs, nil) >= lockers.readQuorum {
		return nil
	}

	log.Println("lockerList.RUnlock() failed.", errs)
	return errReadQuorum
}
