package dory

import (
	"container/list"
	"log"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	megabyte = 1024 * 1024

	maxUintptr = ^uintptr(0)
	maxMemory  = (maxUintptr >> 1)

	freeSearch                = 4
	changedKeysSweepThreshold = 10000
)

var (
	cacheSize = prom.NewGauge(prom.GaugeOpts{
		Name: "dory_cache_size",
		Help: "Size of cache.",
	})
	cacheSizeMax = prom.NewGauge(prom.GaugeOpts{
		Name: "dory_cache_size_max",
		Help: "Maximum cache size.",
	})
	cacheKeys = prom.NewGauge(prom.GaugeOpts{
		Name: "dory_cache_keys",
		Help: "Number of keys in cache.",
	})
)

func init() {
	prom.MustRegister(cacheSize)
	prom.MustRegister(cacheSizeMax)
	prom.MustRegister(cacheKeys)
}

// TODO: Having a pointer here isn't GC friendly.
type keyTable map[uint64]*DiscardableTable

// MemFunc is a function that returns the amount of memory (in bytes) that
// should be used by tables in a Memcache. The usage argument is the bytes of
// memory currently being used by tables.
type MemFunc func(usage int64) int64

// HashFunc is a function which hashes a byte array into a 64-bit hash.
// The hash should have uniform distribution, suitable for use in a hash table.
type HashFunc func(b []byte) uint64

// ConstantMemory returns a MemFunc that causes Memcache to use a fixed amount
// of memory.
func ConstantMemory(size int64) MemFunc {
	return func(int64) int64 {
		return size
	}
}

const (
	DefaultTableSize = 4 * megabyte
	DefaultCacheSize = 64 * megabyte

	DefaultMaxKeySize = 1024
	DefaultMaxValSize = 1024 * 1024
)

type Memcache struct {
	tableSize  int64
	maxKeySize int
	maxValSize int
	memFunc    MemFunc
	hashFunc   HashFunc

	// TODO: Document how this works.
	keys      keyTable
	tables    list.List
	maxTables int
	count     uint64
	lock      sync.Mutex
}

type MemcacheOptions struct {
	MemoryFunction MemFunc
	HashFunction   HashFunc
	TableSize      int
	MaxKeySize     int
	MaxValSize     int
}

func valOrDefault(val, def int) int {
	if val == 0 {
		return def
	}
	return val
}

func NewMemcache(opts MemcacheOptions) *Memcache {
	memFunc := opts.MemoryFunction
	if memFunc == nil {
		memFunc = ConstantMemory(DefaultCacheSize)
	}

	hashFunc := opts.HashFunction
	if hashFunc == nil {
		hashFunc = farm.Hash64
	}

	tableSize := valOrDefault(opts.TableSize, DefaultTableSize)
	if tableSize < 1024 || tableSize > 1<<30 {
		panic("invalid tableSize")
	}

	availableTableMem := memFunc(0)
	if availableTableMem > int64(maxMemory) {
		availableTableMem = int64(maxMemory)
	}
	c := &Memcache{
		tableSize:  int64(tableSize),
		maxKeySize: valOrDefault(opts.MaxKeySize, DefaultMaxKeySize),
		maxValSize: valOrDefault(opts.MaxValSize, DefaultMaxValSize),
		memFunc:    memFunc,
		hashFunc:   hashFunc,
		keys:       make(keyTable),
		maxTables:  int(availableTableMem) / tableSize,
	}
	go c.memWatcher()
	return c
}

func (c *Memcache) MinKeySize() int {
	return 1
}

func (c *Memcache) MinValSize() int {
	return 1
}

func (c *Memcache) MaxKeySize() int {
	return c.maxKeySize
}

func (c *Memcache) MaxValSize() int {
	return c.maxValSize
}

func (c *Memcache) memWatcher() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		c.lock.Lock()
		tableMemUsage := int64(c.tables.Len()) * c.tableSize
		c.lock.Unlock()

		// Do outside lock to avoid blocking.
		availableTableMem := c.memFunc(tableMemUsage)
		if availableTableMem > int64(maxMemory) {
			availableTableMem = int64(maxMemory)
		}

		c.lock.Lock()
		c.maxTables = int(availableTableMem / c.tableSize)
		if c.maxTables < 0 {
			c.maxTables = 0
		}
		c.downsizeTables()
		numTables := int64(c.tables.Len())
		maxTables := int64(c.maxTables)
		numKeys := len(c.keys)
		c.lock.Unlock()

		if debugLog {
			log.Printf("Available table memory: %d MB, tables: %d, max tables: %d",
				availableTableMem/megabyte, numTables, maxTables)
		}

		cacheSize.Set(float64(numTables * c.tableSize))
		cacheSizeMax.Set(float64(maxTables * c.tableSize))
		cacheKeys.Set(float64(numKeys))
	}
}

// Deletes any hash entries that point to |t|.
func (c *Memcache) cleanupTable(t *DiscardableTable) {
	start := time.Now()
	hashes := t.KeyHashes()
	// TODO: This could take a while, so consider spreading the work over many
	// requests.
	for _, h := range hashes {
		if c.keys[h] == t {
			c.erase(h)
		}
	}
	if debugLog {
		log.Printf("cleanupTable hashes: %d, time: %v", len(hashes), time.Since(start))
	}
}

func (c *Memcache) downsizeTables() {
	start := time.Now()
	deleted := 0
	for e := c.tables.Front(); e != nil; {
		next := e.Next()
		t := e.Value.(*DiscardableTable)
		if t.NumEntries() == 0 {
			t.Discard()
			// No call to cleanupTable() here because the table is empty, which
			// implies there are no hashes pointing to it to clean up.
			c.tables.Remove(e)
			deleted++
		}
		e = next
	}
	if debugLog && deleted > 0 {
		log.Printf("Deleted %d empty tables in %0.3f sec", deleted, time.Since(start).Seconds())
	}

	start = time.Now()
	deleted = 0
	for c.tables.Len() > c.maxTables {
		last := c.tables.Back()
		t := last.Value.(*DiscardableTable)
		t.Discard()
		c.cleanupTable(t)
		c.tables.Remove(last)
		deleted++
	}
	if debugLog && deleted > 0 {
		log.Printf("Deleted %d excess tables in %0.3f sec", deleted, time.Since(start).Seconds())
	}

	if debugLog {
		// Count stats.
		liveSpace := 0
		liveEntries := 0
		deletedSpace := 0
		deletedEntries := 0
		freeSpace := 0

		for e := c.tables.Front(); e != nil; e = e.Next() {
			t := e.Value.(*DiscardableTable)
			liveSpace += t.LiveSpace()
			liveEntries += t.NumEntries()
			deletedSpace += t.DeletedSpace()
			deletedEntries += t.NumDeleted()
			freeSpace += t.FreeSpace()
		}
		utilisation := float64(0)
		if c.tables.Len() > 0 {
			utilisation = float64(liveSpace) / (float64(c.tables.Len()) * float64(c.tableSize))
		}

		log.Printf("# tables %d, live (%d/%d MB), deleted (%d/%d MB) free %d MB, utilisation %0.2f",
			c.tables.Len(), liveEntries, liveSpace/megabyte, deletedEntries, deletedSpace/megabyte,
			freeSpace/megabyte, utilisation)
	}
	// TODO: Compact and merge underutilised tables.
}

func (c *Memcache) allocTable() *DiscardableTable {
	t := NewDiscardableTable(int(c.tableSize), c.count)
	c.count++
	if c.count == 0 {
		// Don't bother handling this. Just let the server crash and restart.
		panic("overflow")
	}
	return t
}

func (c *Memcache) recycleTable(old *DiscardableTable) *DiscardableTable {
	t := old.Recycle(c.count)
	c.cleanupTable(old)
	c.count++
	if c.count == 0 {
		// Don't bother handling this. Just let the server crash and restart.
		panic("overflow")
	}

	return t
}

func (c *Memcache) createTable() *DiscardableTable {
	var t *DiscardableTable
	last := c.tables.Back()
	full := (c.tables.Len() >= c.maxTables)

	if last != nil && (full || last.Value.(*DiscardableTable).NumEntries() == 0) {
		t = last.Value.(*DiscardableTable)
		c.tables.Remove(last)
		t = c.recycleTable(t)
	} else {
		t = c.allocTable()
	}
	e := c.tables.PushFront(t)
	t.SetElement(e)

	return t
}

func (c *Memcache) erase(hash uint64) {
	_, ok := c.keys[hash+1]
	if ok {
		// The next hash exists, which _might_ be there due to linear probing.
		// Replace the hash value with nil instead of deleting it, so that key
		// accesses do probing.
		// TODO: Maybe simplify by using a dummy deleted element instead of nil.
		// TODO: Determine whether the next contiguous entries exist due to linear
		// probing, and shuffle them down.
		c.keys[hash] = nil
	} else {
		// No next hash, so no next element for linear probing.
		delete(c.keys, hash)
	}
}

func (c *Memcache) Has(key []byte) bool {
	hash := c.hashFunc(key)
	has := false

	c.lock.Lock()
	for ; !has; hash++ {
		t, ok := c.keys[hash]
		if !ok {
			break
		} else if t == nil {
			continue
		}

		has = t.Has(key)
	}
	c.lock.Unlock()
	return has
}

func (c *Memcache) Get(key, buf []byte) []byte {
	hash := c.hashFunc(key)
	keyHash := hash
	var outBuf []byte

	c.lock.Lock()
	for ; outBuf == nil; hash++ {
		t, ok := c.keys[hash]
		if !ok {
			break
		} else if t == nil {
			continue
		}

		outBuf = t.Get(key)
		if outBuf != nil {
			// Copy value, because Get() returns a slice into its own memory.
			outBuf = append(buf, outBuf...)
			age := (c.count - t.Meta().(uint64))
			if age > freeSearch && age > uint64(c.tables.Len()/2) {
				// Promote old keys to give LRU-like behaviour.
				c.putWithHash(key, outBuf, keyHash)
			}
		}
	}
	c.lock.Unlock()
	return outBuf
}

func (c *Memcache) findPutTable(entrySize int) *DiscardableTable {
	var t *DiscardableTable
	i := 0
	// Search a few of the most recent tables for the smallest spot the entry will fit into.
	for e := c.tables.Front(); e != nil && i < freeSearch; e = e.Next() {
		et := e.Value.(*DiscardableTable)
		if et.FreeSpace() >= entrySize {
			if t == nil || et.FreeSpace() < t.FreeSpace() {
				t = et
			}
		}
		i++
	}
	return t
}

func (c *Memcache) putWithHash(key, val []byte, hash uint64) {
	// Only one copy of the key should exist anywhere in the cache, so
	// deleting any existing value before inserting the new one.
	c.deleteWithHash(key, hash)

	if c.maxTables == 0 {
		return
	}

	if len(key) > c.maxKeySize || len(val) > c.maxValSize {
		return
	}
	entrySize := (*PackedTable)(nil).EntrySize(key, val)

	t := c.findPutTable(entrySize)
	if t == nil {
		t = c.createTable()
	}
	err := t.Put(key, val, hash)
	if err != nil {
		panic(err)
	}
	// Linear probing for the next free hash slot.
	for ; c.keys[hash] != nil; hash++ {
	}
	c.keys[hash] = t
}

func (c *Memcache) Put(key, val []byte) {
	hash := c.hashFunc(key)

	c.lock.Lock()
	c.putWithHash(key, val, hash)
	c.lock.Unlock()
}

func (c *Memcache) tryCompaction(t *DiscardableTable) bool {
	e := t.Element()
	if t.NumEntries() == 0 {
		// Move empty tables to the back to allow them to be recycled.
		t.Reset()
		c.tables.MoveToBack(e)
		// Didn't technically compact, but achieved the same result.
		return true
	}

	// TODO: Compaction.
	return false
}

func (c *Memcache) deleteWithHash(key []byte, hash uint64) {
	for ; ; hash++ {
		t, ok := c.keys[hash]
		if !ok {
			break
		} else if t == nil {
			// Slot for deleted entry.
			// TODO: Consider migrating probed entries closer to their optimal slot.
			continue
		}

		if t.Delete(key) {
			c.erase(hash)
			c.tryCompaction(t)
			// Since the tables are exclusive, we can stop here.
			break
		}
	}
}

func (c *Memcache) Delete(key []byte) {
	hash := c.hashFunc(key)

	c.lock.Lock()
	c.deleteWithHash(key, hash)
	c.lock.Unlock()
}
