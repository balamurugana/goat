package locksys

type LockSys struct {
	lockers     []Locker
	readQuorum  int
	writeQuorum int
}

func NewLockSys(lockers []Locker, readQuorum, writeQuorum int) *LockSys {
	return &LockSys{
		lockers:     lockers,
		readQuorum:  readQuorum,
		writeQuorum: writeQuorum,
	}
}

func (sys *LockSys) GetLocker() Locker {
	lockersCopy := make([]Locker, len(sys.lockers))
	copy(lockersCopy, sys.lockers)

	return newLockerList(lockersCopy, sys.readQuorum, sys.writeQuorum)
}

func (sys *LockSys) GetLockerWithQuorum(readQuorum, writeQuorum int) Locker {
	lockersCopy := make([]Locker, len(sys.lockers))
	copy(lockersCopy, sys.lockers)

	return newLockerList(lockersCopy, readQuorum, writeQuorum)
}
