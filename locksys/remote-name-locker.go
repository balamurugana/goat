package locksys

import (
	"crypto/tls"
	"time"

	xrpc "github.com/balamurugana/goat/pkg/rpc"
)

type NameLockRPCClient struct {
	*RPCClient
}

func (client *NameLockRPCClient) Lock(name string, timeout time.Duration) error {
	return client.Call(nameLockerLock, &LockRPCArgs{Name: name, Timeout: timeout}, &VoidReply{})
}

func (client *NameLockRPCClient) RLock(name string, timeout time.Duration) error {
	return client.Call(nameLockerRLock, &LockRPCArgs{Name: name, Timeout: timeout}, &VoidReply{})
}

func (client *NameLockRPCClient) Unlock(name string) error {
	return client.Call(nameLockerUnlock, &UnlockRPCArgs{Name: name}, &VoidReply{})
}

func (client *NameLockRPCClient) RUnlock(name string) error {
	return client.Call(nameLockerRUnlock, &UnlockRPCArgs{Name: name}, &VoidReply{})
}

func NewNameLockRPCClient(serviceURL string, tlsConfig *tls.Config, rpcVersion RPCVersion) *NameLockRPCClient {
	return &NameLockRPCClient{NewRPCClient(serviceURL, tlsConfig, xrpc.DefaultRPCTimeout, globalRPCAPIVersion)}
}
