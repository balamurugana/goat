package locksys

import (
	"time"

	xrpc "github.com/balamurugana/goat/pkg/rpc"
	xsync "github.com/balamurugana/goat/pkg/sync"
)

const (
	nameLockerServiceName = "NameLocker"
	nameLockerLock        = nameLockerServiceName + ".Lock"
	nameLockerRLock       = nameLockerServiceName + ".RLock"
	nameLockerUnlock      = nameLockerServiceName + ".Unlock"
	nameLockerRUnlock     = nameLockerServiceName + ".RUnlock"
)

type nameLockerRPCReceiver struct {
	local *xsync.NameMutex
}

type LockRPCArgs struct {
	AuthArgs
	Name    string
	Timeout time.Duration
}

func (receiver *nameLockerRPCReceiver) Lock(args *LockRPCArgs, reply *VoidReply) error {
	return receiver.local.Lock(args.Name, args.Timeout)
}

func (receiver *nameLockerRPCReceiver) RLock(args *LockRPCArgs, reply *VoidReply) error {
	return receiver.local.RLock(args.Name, args.Timeout)
}

type UnlockRPCArgs struct {
	AuthArgs
	Name string
}

func (receiver *nameLockerRPCReceiver) Unlock(args *UnlockRPCArgs, reply *VoidReply) error {
	return receiver.local.Unlock(args.Name)
}

func (receiver *nameLockerRPCReceiver) RUnlock(args *UnlockRPCArgs, reply *VoidReply) error {
	return receiver.local.RUnlock(args.Name)
}

func NewNameLockerRPCServer(locker *xsync.NameMutex) *xrpc.Server {
	rpcServer := xrpc.NewServer()
	if err := rpcServer.RegisterName(nameLockerServiceName, &nameLockerRPCReceiver{locker}); err != nil {
		panic(err)
	}

	return rpcServer
}
