package server

import (
	"container/list"
	"runtime"
	"sync"
	"time"
)

const (
	poolGcInterval = time.Minute
)

type poolBuffer struct {
	buf []byte
	t   time.Time
}

type BufferPool struct {
	bufferSize int
	buffers    list.List
	lock       sync.Mutex
}

func NewBufferPool(bufferSize int) *BufferPool {
	p := &BufferPool{
		bufferSize: bufferSize,
	}
	go p.doGc()
	return p
}

func (p *BufferPool) doGc() {
	ticker := time.NewTicker(poolGcInterval)
	for range ticker.C {
		p.lock.Lock()
		didFree := false
		for e := p.buffers.Back(); e != nil; {
			prev := e.Prev()
			if time.Since(e.Value.(poolBuffer).t) > poolGcInterval {
				p.buffers.Remove(e)
				didFree = true
			} else {
				break
			}
			e = prev
		}
		p.lock.Unlock()

		if didFree {
			runtime.GC()
		}
	}
}

func (p *BufferPool) Get() []byte {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.buffers.Len() == 0 {
		return make([]byte, p.bufferSize)
	}

	pb := p.buffers.Remove(p.buffers.Front()).(poolBuffer)
	return pb.buf
}

func (p *BufferPool) Put(buf []byte) {
	if cap(buf) != p.bufferSize {
		return
	}
	buf = buf[:cap(buf)]

	p.lock.Lock()
	defer p.lock.Unlock()
	p.buffers.PushFront(poolBuffer{buf, time.Now()})
}
