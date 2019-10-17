package rpc

import (
	"net"
	"time"
)

// conn with read/write idle timeout.
type timeoutConn struct {
	net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (c *timeoutConn) Read(b []byte) (n int, err error) {
	if c.readTimeout != 0 {
		c.SetReadDeadline(time.Now().UTC().Add(c.readTimeout))
	}

	return c.Conn.Read(b)
}

func (c *timeoutConn) Write(b []byte) (n int, err error) {
	if c.writeTimeout != 0 {
		c.SetWriteDeadline(time.Now().UTC().Add(c.writeTimeout))
	}

	return c.Conn.Write(b)
}

func newTimeoutConn(conn net.Conn, readTimeout, writeTimeout time.Duration) *timeoutConn {
	return &timeoutConn{
		Conn:         conn,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}
