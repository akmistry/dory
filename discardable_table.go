package dory

import (
	"container/list"
)

// TODO: Rename to MmappedTable?
type DiscardableTable struct {
	table   *PackedTable
	buf     []byte
	meta    interface{}
	element *list.Element

	keyHashes []uint64
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
}

func (t *DiscardableTable) Reset() {
	if t.table == nil {
		panic("t.table == nil")
	}
	t.table.Reset()
	t.keyHashes = nil
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

func (t *DiscardableTable) Put(key, val []byte, hash uint64) error {
	if t.table == nil {
		return nil
	}
	t.keyHashes = append(t.keyHashes, hash)
	return t.table.Put(key, val)
}

func (t *DiscardableTable) Delete(key []byte) bool {
	if t.table == nil {
		return false
	}
	return t.table.Delete(key)
}

func (t *DiscardableTable) KeyHashes() []uint64 {
	return t.keyHashes
}
