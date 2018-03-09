package dory

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/dgryski/go-farm"
)

var (
	ErrNoSpace = errors.New("insufficent space left")
)

// PackedTable is a simple key/value table that stores key and value data
// contiguously within a single []byte slice. The only data stored outside the
// slice is an index used to locate entries in the slice.
//
// The primary goals are to minimise heap fragmentation, and allow the user to
// instantly free memory used by this table (i.e. using munmap()). A good side
// effect is to also reduce Go GC pressure, although this is not a primary
// goal.
//
// Note: PackedTable is not thread-safe.
type PackedTable struct {
	buf             []byte
	autoGcThreshold int
	off             int
	keys            map[uint32]int32

	added        int
	deleted      int
	deletedSpace int
}

// Construct a new PackedTable using the given slice to store key/value data.
// autoGcThreshold is the number of bytes of data deleted before the table
// automatically performs a garbage collection (technically a compaction).
// If autoGcThreshold is 0, automatic GC is disabled.
// Note: The slice MUST be smalled than 1GiB in length.
func NewPackedTable(buf []byte, autoGcThreshold int) *PackedTable {
	if len(buf) > 1<<30 {
		panic("len(buf) > 1GiB")
	}

	return &PackedTable{
		buf:             buf,
		autoGcThreshold: autoGcThreshold,
		keys:            make(map[uint32]int32),
	}
}

// Reset erases all data in the table.
func (t *PackedTable) Reset() {
	t.off = 0
	t.added = 0
	t.deleted = 0
	t.deletedSpace = 0
	t.keys = make(map[uint32]int32)
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
		keyOff := int(off) + 8
		if keySize == len(key) && bytes.Compare(key, t.buf[keyOff:keyOff+len(key)]) == 0 {
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
		keyOff := int(off) + 8
		if keySize == len(key) && bytes.Compare(key, t.buf[keyOff:keyOff+len(key)]) == 0 {
			return int(off)
		}
	}
	return -1
}

// EntrySize returns the amount of space used in the table's slice by the given
// key/value. May be used to determine if there is sufficient space to store
// the key/value.
func (t *PackedTable) EntrySize(key, val []byte) int {
	return len(key) + len(val) + 8
}

// FreeSpace returns the number of bytes of usable free space in the table.
func (t *PackedTable) FreeSpace() int {
	return len(t.buf) - t.off
}

// LiveSpace return the number of bytes used by entries in the table.
func (t *PackedTable) LiveSpace() int {
	return t.off - t.deletedSpace
}

// DeletedSpace returns the number of bytes used by deleted entries in the
// table. This space may be reclaimed by running a garbage collection.
// Note: LiveSpace + FreeSpace + DeletedSpace == len(buf)
func (t *PackedTable) DeletedSpace() int {
	return t.deletedSpace
}

// NumEntries returns the number of entries in the table. Should probably be
// renamed to Len().
func (t *PackedTable) NumEntries() int {
	return t.added - t.deleted
}

// NumDeleted returns the number of deleted entries in the table. This space
// may be reclaimed by running a garbage collection.
func (t *PackedTable) NumDeleted() int {
	return t.deleted
}

// Has returns whether or not the table contains the requested key.
func (t *PackedTable) Has(key []byte) bool {
	if len(key) == 0 {
		panic("zero-sized key")
	}

	return t.findKey(key) >= 0
}

// Get returns a slice of the value for the key, if it exists in the table,
// or nil if the key does not exist. The returned slice will be a slice into
// the table's memory and MUST NOT be modified, and is only valid until the
// next call into PackedTable.
func (t *PackedTable) Get(key []byte) []byte {
	if len(key) == 0 {
		panic("zero-sized key")
	}

	off := t.findKey(key)
	if off < 0 {
		return nil
	}

	keySize, valSize := t.readSize(off)
	valOff := off + 8 + keySize
	return t.buf[valOff : valOff+valSize]
}

// Put adds the key/value into the table, if there is sufficient free space.
// Returns nil on success, or ErrNoSpace if there is insufficient free space.
// If the table already contains the key, the existing key/value will be
// deleted (as if Delete() was called), and the new entry inserted.
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
		keySize, valSize := t.readSize(int(off))
		// So that the key doesn't get caught up in GC.
		t.keys[hash] = -1
		t.deleted++
		t.deletedSpace += keySize + valSize + 8
		t.autoGc()
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

// Delete removes the key, and returns true if the key existed. Deleting a
// key may not automatically free space used by that key/value. Space is only
// reclaimed if the amount of deleted space exceeds the auto-GC threshold, or
// a GC is explicitly performed.
func (t *PackedTable) Delete(key []byte) bool {
	if len(key) == 0 {
		panic("zero-sized key")
	}

	hash := t.hashEntry(key)
	off, ok := t.keys[hash]
	if ok && off >= 0 {
		keySize, valSize := t.readSize(int(off))
		_, ok := t.keys[hash+1]
		if ok {
			t.keys[hash] = -1
		} else {
			delete(t.keys, hash)
		}
		t.deleted++
		t.deletedSpace += keySize + valSize + 8
		t.autoGc()
		return true
	}
	return false
}

func (t *PackedTable) autoGc() {
	if t.autoGcThreshold > 0 && t.deletedSpace > t.autoGcThreshold {
		t.GC()
	}
}

// GC performs a garbage collection to reclaim free space.
func (t *PackedTable) GC() {
	if t.deleted == 0 {
		// No deleted entries => no GC needed.
		return
	}

	type hashEntry struct {
		hash uint32
		off  int
	}
	entries := make([]hashEntry, 0, t.NumEntries())
	for h, off := range t.keys {
		if off < 0 {
			continue
		}
		entries = append(entries, hashEntry{h, int(off)})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].off < entries[j].off
	})

	t.added = 0
	t.deleted = 0
	t.deletedSpace = 0
	t.off = 0
	prevOff := -1
	for _, e := range entries {
		if e.off <= prevOff {
			panic("e.off <= prevOff")
		} else if e.off < t.off {
			panic("e.off < t.off")
		}
		keySize, valSize := t.readSize(e.off)
		entrySize := keySize + valSize + 8
		copy(t.buf[t.off:], t.buf[e.off:e.off+entrySize])
		t.keys[e.hash] = int32(t.off)
		t.off += entrySize
		t.added++
		prevOff = e.off
	}
}
