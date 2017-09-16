package server

import (
	"container/list"
	"math"

	"golang.org/x/net/http2"
)

const (
	writeQueueFreeListSize = 8
	maxWriteQueueLen       = 8
)

type writeQueue struct {
	id uint32
	q  []http2.FrameWriteRequest
}

func (q *writeQueue) len() int {
	return len(q.q)
}

func (q *writeQueue) front() http2.FrameWriteRequest {
	return q.q[0]
}

func (q *writeQueue) popFront() {
	copy(q.q, q.q[1:])
	last := len(q.q) - 1
	q.q[last] = http2.FrameWriteRequest{}
	q.q = q.q[:last]

	// TODO: Detect queue with too much empty space and reduce.
}

type roundRobinScheduler struct {
	zq writeQueue

	streams      map[uint32]*list.Element
	pendingQueue list.List

	freeList []*writeQueue
}

func NewRoundRobinScheduler() http2.WriteScheduler {
	return &roundRobinScheduler{
		streams:  make(map[uint32]*list.Element),
		freeList: make([]*writeQueue, 0, writeQueueFreeListSize),
	}
}

func (s *roundRobinScheduler) OpenStream(streamID uint32, options http2.OpenStreamOptions) {
	// No-op.
}

func (s *roundRobinScheduler) getWriteQueue() *writeQueue {
	if len(s.freeList) > 0 {
		last := len(s.freeList) - 1
		wq := s.freeList[last]
		s.freeList[last] = nil // Avoid leaks.
		s.freeList = s.freeList[:last]
		return wq
	}
	return new(writeQueue)
}

func (s *roundRobinScheduler) freeWriteQueue(wq *writeQueue) {
	if len(s.freeList) < cap(s.freeList) {
		if cap(wq.q) > maxWriteQueueLen {
			wq.q = nil
		} else {
			wq.q = wq.q[:0]
		}
		s.freeList = append(s.freeList, wq)
	}
}

func (s *roundRobinScheduler) CloseStream(streamID uint32) {
	e := s.streams[streamID]
	if e != nil {
		s.freeWriteQueue(e.Value.(*writeQueue))
		s.pendingQueue.Remove(e)
		delete(s.streams, streamID)
	}
}

func (s *roundRobinScheduler) AdjustStream(streamID uint32, priority http2.PriorityParam) {
	// No-op.
}

func (s *roundRobinScheduler) Push(wr http2.FrameWriteRequest) {
	id := wr.StreamID()
	if id == 0 {
		s.zq.q = append(s.zq.q, wr)
		return
	}

	e := s.streams[id]
	if e == nil {
		wq := s.getWriteQueue()
		wq.id = id
		e = s.pendingQueue.PushBack(wq)
		s.streams[id] = e
	}
	wq := e.Value.(*writeQueue)
	wq.q = append(wq.q, wr)
}

func (s *roundRobinScheduler) Pop() (wr http2.FrameWriteRequest, ok bool) {
	if s.zq.len() > 0 {
		wr = s.zq.front()
		s.zq.popFront()
		return wr, true
	}

	i := 0
	origLen := s.pendingQueue.Len()
	for e := s.pendingQueue.Front(); e != nil && i < origLen; {
		i++
		next := e.Next()

		q := e.Value.(*writeQueue)
		if len(q.q) == 0 {
			s.freeWriteQueue(q)
			delete(s.streams, q.id)
			s.pendingQueue.Remove(e)
			e = next
		} else {
			consumed, rest, count := q.front().Consume(math.MaxInt32)
			s.pendingQueue.MoveToBack(e)
			switch count {
			case 0:
				// No bytes consumed.
				e = next
				continue
			case 1:
				// Entire frame consumed.
				q.popFront()
				return consumed, true
			case 2:
				// Partial frame consumed. Push rest to front.
				q.q[0] = rest
				return consumed, true
			}
		}
	}

	return http2.FrameWriteRequest{}, false
}
