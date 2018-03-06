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

type KeyTable map[uint64]*DiscardableTable

// Sentinel value to indicate table entry has been deleted.
var deletedEntry = new(DiscardableTable)

type Memcache struct {
	minFreeMem int64
	tableSize  int
	maxKeySize int
	maxValSize int

	doSweepKeys chan KeyTable

	// TODO: Document how this works.
	keys        KeyTable
	changedKeys KeyTable
	tables      list.List
	maxTables   int
	count       int
	lock        sync.Mutex
}

func NewMemcache(minFreeMem int64, tableSize, maxKeySize, maxValSize int) *Memcache {
	c := &Memcache{
		minFreeMem:  minFreeMem,
		tableSize:   tableSize,
		maxKeySize:  maxKeySize,
		maxValSize:  maxValSize,
		doSweepKeys: make(chan KeyTable, 1),
		keys:        make(KeyTable),
		changedKeys: make(KeyTable),
	}
	go c.memWatcher()
	go c.sweepKeys()
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
		memAvailable := getMemAvailable()

		c.lock.Lock()
		availableTableMem := int64(c.tables.Len()*c.tableSize) + memAvailable - c.minFreeMem
		if availableTableMem > int64(maxMemory) {
			availableTableMem = int64(maxMemory)
		}
		c.maxTables = int(availableTableMem / int64(c.tableSize))
		if c.maxTables < 0 {
			c.maxTables = 0
		}
		c.downsizeTables()
		numTables := c.tables.Len()
		maxTables := c.maxTables
		numKeys := len(c.keys)
		c.lock.Unlock()

		if debugLog {
			log.Printf("Mem avail: %d MB, table mem available: %d MB, tables: %d, max tables: %d",
				memAvailable/megabyte, availableTableMem/megabyte, numTables, maxTables)
		}

		cacheSize.Set(float64(numTables * c.tableSize))
		cacheSizeMax.Set(float64(maxTables * c.tableSize))
		cacheKeys.Set(float64(numKeys))
	}
}

func (c *Memcache) sweepKeys() {
	// Operate on a copy of the key map to minimise blocking.
	keysCopy := make(KeyTable)
	// Cap the amount of work done while holding c.lock per iteration.
	nils := make([]uint64, 0, 10000)

	for changed := range c.doSweepKeys {
		nils = nils[:0]

		sweepStart := time.Now()

		// Merge changes into our copy.
		for k, t := range changed {
			if t == deletedEntry {
				delete(keysCopy, k)
			} else {
				keysCopy[k] = t
			}
		}
		// Look for candidate keys to verify whether they're still valid.
		for k, t := range keysCopy {
			if t == nil || !t.IsAlive() {
				nils = append(nils, k)
				if len(nils) == cap(nils) {
					break
				}
			}
		}

		if len(nils) > 0 {
			c.lock.Lock()
			start := time.Now()
			numKeys := len(c.keys)
			for _, k := range nils {
				t, ok := c.keys[k]
				if !ok {
					delete(keysCopy, k)
				} else if t == nil || !t.IsAlive() {
					c.erase(k)
				}
			}
			deleted := numKeys - len(c.keys)

			if len(nils) == cap(nils) {
				// More work to be done.
				c.doSweep()
			}

			c.lock.Unlock()

			if debugLog {
				log.Printf("Swept %d keys in %0.6f sec, deleted %d, nils %d, total sweep time %0.3f sec",
					numKeys, time.Since(start).Seconds(), deleted, len(nils), time.Since(sweepStart).Seconds())
			}
		} else if debugLog {
			log.Printf("No nil entries to sweep, key copies %d, total sweep time %0.3f sec",
				len(keysCopy), time.Since(sweepStart).Seconds())
		}

	}
}

func (c *Memcache) doSweep() {
	select {
	case c.doSweepKeys <- c.changedKeys:
		c.changedKeys = make(KeyTable)
	default:
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
		c.tables.Remove(last)
		deleted++
	}
	if deleted > 0 {
		// Discarding non-empty tables creates orphaned key table entries which need
		// to be swept away.
		c.doSweep()
	}
	if debugLog && deleted > 0 {
		log.Printf("Deleted %d excess tables in %0.3f sec", deleted, time.Since(start).Seconds())
	}

	// TODO: Compact and merge underutilised tables.
}

func (c *Memcache) allocTable() *DiscardableTable {
	t := NewDiscardableTable(c.tableSize, c.count)
	c.count++
	return t
}

func (c *Memcache) keyChanged(hash uint64, t *DiscardableTable) {
	c.changedKeys[hash] = t
	if len(c.changedKeys) > changedKeysSweepThreshold {
		c.doSweep()
	}
}

func (c *Memcache) erase(hash uint64) {
	_, ok := c.keys[hash+1]
	if ok {
		// TODO: Maybe simplify by using a dummy deleted element instead of nil.
		c.keys[hash] = nil
		c.keyChanged(hash, nil)
	} else {
		// No next hash, so no next element for linear probing.
		delete(c.keys, hash)
		c.keyChanged(hash, deletedEntry)
	}
}

func (c *Memcache) Has(key []byte) bool {
	hash := farm.Hash64(key)
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
	hash := farm.Hash64(key)
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

		outBuf = t.Get(key, buf)
		if outBuf != nil {
			if (c.count - t.Meta().(int)) > c.maxTables/2 {
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
		if c.tables.Len() >= c.maxTables {
			last := c.tables.Back()
			bt := last.Value.(*DiscardableTable)
			bt.Discard()
			c.tables.Remove(last)
			c.doSweep()
		}
		t = c.allocTable()
		c.tables.PushFront(t)
	}
	err := t.Put(key, val)
	if err != nil {
		panic(err)
	}
	for ; c.keys[hash] != nil; hash++ {
	}
	c.keys[hash] = t
	c.keyChanged(hash, t)
}

func (c *Memcache) Put(key, val []byte) {
	hash := farm.Hash64(key)

	c.lock.Lock()
	c.putWithHash(key, val, hash)
	c.lock.Unlock()
}

func (c *Memcache) deleteWithHash(key []byte, hash uint64) {
	for ; ; hash++ {
		t, ok := c.keys[hash]
		if !ok {
			break
		} else if t == nil || !t.IsAlive() {
			// While we're here, might as well clean out the garbage.
			c.erase(hash)
			continue
		}

		if t.Delete(key) {
			c.erase(hash)
			// Since the tables are exclusive, we can stop here.
			break
		}
	}
}

func (c *Memcache) Delete(key []byte) {
	hash := farm.Hash64(key)

	c.lock.Lock()
	c.deleteWithHash(key, hash)
	c.lock.Unlock()
}
