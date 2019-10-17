package rpc

import (
	"io"
	"io/ioutil"
	"sync/atomic"
)

type drainReader struct {
	rc       io.ReadCloser
	isClosed int32
}

func newDrainReader(rc io.ReadCloser) *drainReader {
	return &drainReader{rc: rc}
}

func (dr *drainReader) Read(p []byte) (n int, err error) {
	return dr.rc.Read(p)
}

func (dr *drainReader) Close() (err error) {
	if atomic.SwapInt32(&dr.isClosed, 1) == 0 {
		go func() {
			io.Copy(ioutil.Discard, dr.rc)
			dr.rc.Close()
		}()
	}

	return nil
}
