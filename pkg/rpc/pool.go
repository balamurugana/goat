package rpc

import (
	"bytes"
	"sync"
)

var bufPool = newBufPool()

type bufPoolType struct {
	p *sync.Pool
}

func (p bufPoolType) Get() *bytes.Buffer {
	return p.p.Get().(*bytes.Buffer)
}

func (p bufPoolType) Put(buf *bytes.Buffer) {
	buf.Reset()
	p.p.Put(buf)
}

func newBufPool() bufPoolType {
	return bufPoolType{p: &sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}}
}
