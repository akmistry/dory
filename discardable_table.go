package dory

import (
	"container/list"
	"sync/atomic"
)

// TODO: Rename to MmappedTable?
type DiscardableTable struct {
	table   *PackedTable
	buf     []byte
	meta    interface{}
	element *list.Element

	dead atomic.Bool
}

func NewDiscardableTable(size int, meta interface{}) *DiscardableTable {
	buf, err := mmap(size)
	if err != nil {
		panic(err)
	}
	return &DiscardableTable{
		table: NewPackedTable(buf, len(buf)/4),
		buf:   buf,
		meta:  meta,
	}
}

func (t *DiscardableTable) Recycle(meta interface{}) *DiscardableTable {
	if t.table == nil {
		panic("t.table == nil")
	}
	newTable := &DiscardableTable{
		table: NewPackedTable(t.buf, len(t.buf)/4),
		buf:   t.buf,
		meta:  meta,
	}
	t.table = nil
	t.buf = nil
	t.dead.Store(true)
	return newTable
}

func (t *DiscardableTable) Meta() interface{} {
	return t.meta
}

func (t *DiscardableTable) SetElement(e *list.Element) {
	t.element = e
}

func (t *DiscardableTable) Element() *list.Element {
	return t.element
}

func (t *DiscardableTable) Discard() {
	if t.table == nil {
		return
	}
	err := munmap(t.buf)
	if err != nil {
		panic(err)
	}
	t.table = nil
	t.buf = nil
	t.dead.Store(true)
}

func (t *DiscardableTable) Reset() {
	if t.table == nil {
		panic("t.table == nil")
	}
	t.table.Reset()
}

func (t *DiscardableTable) IsDead() bool {
	// This function can be called without locking. But case must be taken when
	// doing so. Without the external locking, there's no memory state to wait on
	// (i.e. a mutex lock/unlock) to ensure the atomic becomes visible on another
	// CPU (except for the external synchronisation during initialisation). This
	// means there are two possible states:
	// 1. dead==true => Table is definetly dead.
	// 2. dead==false => Table might be alive or dead. We can't know unless the
	//    same lock used to call Recycle() or Discard() is taken while calling
	//    this function.
	return t.dead.Load()
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

func (t *DiscardableTable) DeletedSpace() int {
	if t.table == nil {
		panic("t.table == nil")
	}
	return t.table.DeletedSpace()
}

func (t *DiscardableTable) Has(key []byte) bool {
	if t.table == nil {
		return false
	}
	return t.table.Has(key)
}

func (t *DiscardableTable) Get(key []byte) []byte {
	if t.table == nil {
		return nil
	}
	return t.table.Get(key)
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
