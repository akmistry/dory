package server

import (
	"container/list"
	"math"

	"golang.org/x/net/http2"
)

type writeQueue struct {
	id uint32
	q  list.List
}

type roundRobinScheduler struct {
	zq writeQueue

	streams      map[uint32]*list.Element
	pendingQueue list.List
}

func NewRoundRobinScheduler() http2.WriteScheduler {
	return &roundRobinScheduler{streams: make(map[uint32]*list.Element)}
}

func (s *roundRobinScheduler) OpenStream(streamID uint32, options http2.OpenStreamOptions) {
	// No-op.
}

func (s *roundRobinScheduler) CloseStream(streamID uint32) {
	e := s.streams[streamID]
	if e != nil {
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
		s.zq.q.PushBack(wr)
		return
	}

	e := s.streams[id]
	if e == nil {
		e = s.pendingQueue.PushBack(&writeQueue{id: id})
		s.streams[id] = e
	}
	e.Value.(*writeQueue).q.PushBack(wr)
}

func (s *roundRobinScheduler) Pop() (wr http2.FrameWriteRequest, ok bool) {
	if s.zq.q.Len() > 0 {
		e := s.zq.q.Front()
		s.zq.q.Remove(e)
		return e.Value.(http2.FrameWriteRequest), true
	}

	i := 0
	for e := s.pendingQueue.Front(); e != nil && i < s.pendingQueue.Len(); {
		i++
		next := e.Next()

		q := e.Value.(*writeQueue)
		if q.q.Len() == 0 {
			delete(s.streams, q.id)
			s.pendingQueue.Remove(e)
			e = next
		} else {
			wqe := q.q.Front()
			consumed, rest, count := wqe.Value.(http2.FrameWriteRequest).Consume(math.MaxInt32)
			s.pendingQueue.MoveToBack(e)
			switch count {
			case 0:
				// No bytes consumed.
				e = next
				continue
			case 1:
				// Entire frame consumed.
				q.q.Remove(wqe)
				return consumed, true
			case 2:
				// Partial frame consumed. Push rest to front.
				q.q.Remove(wqe)
				q.q.PushFront(rest)
				return consumed, true
			}
		}
	}

	return http2.FrameWriteRequest{}, false
}
