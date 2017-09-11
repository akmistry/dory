package dory

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/dgryski/go-farm"
)

var (
	ErrNoSpace = errors.New("insufficent space left")
)

type PackedTable struct {
	buf  []byte
	off  int
	keys map[uint32]int32

	added   int
	deleted int
}

func NewPackedTable(buf []byte) *PackedTable {
	if len(buf) > 1<<30 {
		panic("len(buf) > 1GiB")
	}

	return &PackedTable{
		buf:  buf,
		keys: make(map[uint32]int32),
	}
}

func (t *PackedTable) hashKey(key []byte) uint32 {
	return farm.Hash32(key)
}

func (t *PackedTable) readSize(off int) (int, int) {
	// TODO: Use "unsafe"?
	keySize := binary.LittleEndian.Uint32(t.buf[off:])
	valSize := binary.LittleEndian.Uint32(t.buf[off+4:])
	return int(keySize), int(valSize)
}

func (t *PackedTable) writeSize(key, val int) int {
	off := t.off
	t.off += 8
	binary.LittleEndian.PutUint32(t.buf[off:], uint32(key))
	binary.LittleEndian.PutUint32(t.buf[off+4:], uint32(val))
	return off
}

func (t *PackedTable) hashEntry(key []byte) uint32 {
	hash := t.hashKey(key)
	for ; ; hash++ {
		off, ok := t.keys[hash]
		if !ok {
			break
		} else if off < 0 {
			continue
		}

		keySize, _ := t.readSize(int(off))
		if keySize == len(key) && bytes.Compare(key, t.buf[int(off)+8:int(off)+8+len(key)]) == 0 {
			break
		}
	}
	return hash
}

func (t *PackedTable) findKey(key []byte) int {
	hash := t.hashKey(key)
	for ; ; hash++ {
		off, ok := t.keys[hash]
		if !ok {
			break
		} else if off < 0 {
			continue
		}

		keySize, _ := t.readSize(int(off))
		if keySize == len(key) && bytes.Compare(key, t.buf[int(off)+8:int(off)+8+len(key)]) == 0 {
			return int(off)
		}
	}
	return -1
}

func (t *PackedTable) EntrySize(key, val []byte) int {
	return len(key) + len(val) + 8
}

func (t *PackedTable) FreeSpace() int {
	return len(t.buf) - t.off
}

func (t *PackedTable) NumEntries() int {
	return t.added - t.deleted
}

func (t *PackedTable) NumDeleted() int {
	return t.deleted
}

func (t *PackedTable) Has(key []byte) bool {
	if len(key) == 0 {
		panic("zero-sized key")
	}

	return t.findKey(key) >= 0
}

func (t *PackedTable) Get(key, buf []byte) []byte {
	if len(key) == 0 {
		panic("zero-sized key")
	}

	off := t.findKey(key)
	if off < 0 {
		return nil
	}

	keySize, valSize := t.readSize(off)
	buf = append(buf, t.buf[off+8+keySize:off+8+keySize+valSize]...)
	return buf
}

func (t *PackedTable) Put(key, val []byte) error {
	if len(key) == 0 {
		panic("zero-sized key")
	}

	size := t.EntrySize(key, val)
	if size > t.FreeSpace() {
		return ErrNoSpace
	}

	hash := t.hashEntry(key)
	if off, ok := t.keys[hash]; ok && off >= 0 {
		t.deleted++
	}

	off := t.writeSize(len(key), len(val))
	n := copy(t.buf[t.off:], key)
	t.off += n
	if n != len(key) {
		panic("n != len(key)")
	}
	n = copy(t.buf[t.off:], val)
	t.off += n
	if n != len(val) {
		panic("n != len(val)")
	}
	t.keys[hash] = int32(off)
	t.added++
	return nil
}

func (t *PackedTable) Delete(key []byte) bool {
	if len(key) == 0 {
		panic("zero-sized key")
	}

	hash := t.hashEntry(key)
	off, ok := t.keys[hash]
	if ok && off >= 0 {
		_, ok := t.keys[hash+1]
		if ok {
			t.keys[hash] = -1
		} else {
			delete(t.keys, hash)
		}
		t.deleted++
		return true
	}
	return false
}
