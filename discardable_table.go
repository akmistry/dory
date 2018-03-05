package dory

import (
	"sync/atomic"
	"syscall"
)

type DiscardableTable struct {
	table *PackedTable
	buf   []byte
	meta  interface{}
	dead  int32
}

func NewDiscardableTable(size int, meta interface{}) *DiscardableTable {
	buf, err := syscall.Mmap(0, 0, size, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS|syscall.MAP_POPULATE)
	if err != nil {
		panic(err)
	}
	return &DiscardableTable{
		table: NewPackedTable(buf, len(buf)/4),
		buf:   buf,
		meta:  meta,
	}
}

func (t *DiscardableTable) Meta() interface{} {
	return t.meta
}

func (t *DiscardableTable) Discard() {
	if t.table == nil {
		return
	}
	err := syscall.Munmap(t.buf)
	if err != nil {
		panic(err)
	}
	t.table = nil
	t.buf = nil
	atomic.StoreInt32(&t.dead, 1)
}

func (t *DiscardableTable) Reset() {
	if t.table == nil {
		panic("t.table == nil")
	}
	t.table.Reset()
}

func (t *DiscardableTable) IsAlive() bool {
	return atomic.LoadInt32(&t.dead) == 0
}

func (t *DiscardableTable) NumEntries() int {
	if t.table == nil {
		panic("t.table == nil")
	}
	return t.table.NumEntries()
}

func (t *DiscardableTable) NumDeleted() int {
	if t.table == nil {
		panic("t.table == nil")
	}
	return t.table.NumDeleted()
}

func (t *DiscardableTable) FreeSpace() int {
	if t.table == nil {
		panic("t.table == nil")
	}
	return t.table.FreeSpace()
}

func (t *DiscardableTable) LiveSpace() int {
	if t.table == nil {
		panic("t.table == nil")
	}
	return t.table.LiveSpace()
}

func (t *DiscardableTable) Has(key []byte) bool {
	if t.table == nil {
		return false
	}
	return t.table.Has(key)
}

func (t *DiscardableTable) Get(key, buf []byte) []byte {
	if t.table == nil {
		return nil
	}
	return t.table.Get(key, buf)
}

func (t *DiscardableTable) Put(key, val []byte) error {
	if t.table == nil {
		return nil
	}
	return t.table.Put(key, val)
}

func (t *DiscardableTable) Delete(key []byte) bool {
	if t.table == nil {
		return false
	}
	return t.table.Delete(key)
}
