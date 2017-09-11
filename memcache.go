package dory

import (
	"bufio"
	"bytes"
	"container/list"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dgryski/go-farm"
	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	megabyte = 1024 * 1024
)

var (
	cacheSize = prom.NewGauge(prom.GaugeOpts{
		Name: "dory_cache_size",
		Help: "Size of cache",
	})
	cacheSizeMax = prom.NewGauge(prom.GaugeOpts{
		Name: "dory_cache_size_max",
		Help: "Maximum cache size",
	})
)

func init() {
	prom.MustRegister(cacheSize)
	prom.MustRegister(cacheSizeMax)
}

type memcacheTable struct {
	table   *PackedTable
	mmapBuf []byte
	gen     int
}

func newMemcacheTable(size, gen int) *memcacheTable {
	// TODO: Refactor.
	buf, err := syscall.Mmap(0, 0, size, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS|syscall.MAP_POPULATE)
	if err != nil {
		panic(err)
	}
	return &memcacheTable{
		table:   NewPackedTable(buf),
		mmapBuf: buf,
		gen:     gen,
	}
}

func (t *memcacheTable) free() {
	if t.table == nil {
		return
	}
	err := syscall.Munmap(t.mmapBuf)
	if err != nil {
		panic(err)
	}
	t.table = nil
	t.mmapBuf = nil
}

type Memcache struct {
	minFreeMem int
	tableSize  int
	maxKeySize int
	maxValSize int

	// TODO: Document how this works.
	keys      map[uint64]*memcacheTable
	tables    list.List
	maxTables int
	count     int
	lock      sync.Mutex
}

func NewMemcache(minFreeMem, tableSize, maxKeySize, maxValSize int) *Memcache {
	c := &Memcache{
		minFreeMem: minFreeMem,
		tableSize:  tableSize,
		maxKeySize: maxKeySize,
		maxValSize: maxValSize,
		keys:       make(map[uint64]*memcacheTable),
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
		// TODO: Refactor.
		meminfo, err := ioutil.ReadFile("/proc/meminfo")
		if err != nil {
			panic(err)
		}
		memAvailable := 0
		r := bufio.NewReader(bytes.NewReader(meminfo))
		line, _ := r.ReadString('\n')
		for line != "" {
			if strings.HasPrefix(line, "MemAvailable:") {
				remain := strings.TrimSpace(line[len("MemAvailable:"):])
				var units string
				_, err = fmt.Sscanf(remain, "%d %s", &memAvailable, &units)
				if err != nil {
					panic(err)
				}
				if units == "kB" {
					memAvailable *= 1024
				}
				break
			}
			line, _ = r.ReadString('\n')
		}

		c.lock.Lock()
		availableTableMem := c.tables.Len()*c.tableSize + memAvailable - c.minFreeMem
		c.maxTables = availableTableMem / c.tableSize
		if c.maxTables < 0 {
			c.maxTables = 0
		}
		c.downsizeTables()
		numTables := c.tables.Len()
		maxTables := c.maxTables
		c.lock.Unlock()

		if debugLog {
			log.Printf("Mem avail: %d MB, table mem available: %d MB, tables: %d, max tables: %d",
				memAvailable/megabyte, availableTableMem/megabyte, numTables, maxTables)
		}

		cacheSize.Set(float64(numTables * c.tableSize))
		cacheSizeMax.Set(float64(maxTables * c.tableSize))
	}
}

func (c *Memcache) sweepKeys() {
	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		c.lock.Lock()
		start := time.Now()
		deleted := 0
		ks := make([]uint64, 0, len(c.keys))
		for k, _ := range c.keys {
			ks = append(ks, k)
		}
		for _, k := range ks {
			t, ok := c.keys[k]
			if !ok {
				continue
			} else if t == nil || t.table == nil {
				c.erase(k)
				deleted++
			}
		}
		if debugLog {
			log.Printf("Swept %d keys in %0.2f sec, deleted %d",
				len(ks), time.Since(start).Seconds(), deleted)
		}
		c.lock.Unlock()
	}
}

func (c *Memcache) downsizeTables() {
	start := time.Now()
	deleted := 0
	for e := c.tables.Front(); e != nil; {
		t := e.Value.(*memcacheTable)
		if t.table.NumEntries() == 0 {
			t.free()
			curr := e
			e = e.Next()
			c.tables.Remove(curr)
			deleted++
		} else {
			e = e.Next()
		}
	}
	if debugLog && deleted > 0 {
		log.Printf("Deleted %d empty tables in %0.2f sec", deleted, time.Since(start).Seconds())
	}

	start = time.Now()
	deleted = 0
	for c.tables.Len() > c.maxTables {
		last := c.tables.Back()
		t := last.Value.(*memcacheTable)
		t.free()
		c.tables.Remove(last)
	}
	if debugLog && deleted > 0 {
		log.Printf("Deleted %d excess tables in %0.2f sec", deleted, time.Since(start).Seconds())
	}

	// TODO: Compact and merge underutilised tables.
}

func (c *Memcache) allocTable() *memcacheTable {
	t := newMemcacheTable(c.tableSize, c.count)
	c.count++
	return t
}

func (c *Memcache) erase(hash uint64) {
	_, ok := c.keys[hash+1]
	if ok {
		// TODO: Maybe simplify be using a dummy deleted element instead of nil.
		c.keys[hash] = nil
	} else {
		// No next hash, so no next element for linear probing.
		delete(c.keys, hash)
	}
}

func (c *Memcache) Has(key []byte) bool {
	hash := farm.Hash64(key)

	c.lock.Lock()
	defer c.lock.Unlock()
	for ; ; hash++ {
		t, ok := c.keys[hash]
		if !ok {
			break
		} else if t == nil || t.table == nil {
			continue
		}

		if t.table.Has(key) {
			return true
		}
	}
	return false
}

func (c *Memcache) Get(key, buf []byte) []byte {
	hash := farm.Hash64(key)
	keyHash := hash

	c.lock.Lock()
	defer c.lock.Unlock()
	for ; ; hash++ {
		t, ok := c.keys[hash]
		if !ok {
			break
		} else if t == nil || t.table == nil {
			continue
		}

		outBuf := t.table.Get(key, buf)
		if outBuf != nil {
			if (c.count - t.gen) > c.maxTables/2 {
				// Promote old keys to give LRU-like behaviour.
				c.putWithHash(key, outBuf, keyHash)
			}
			return outBuf
		}
	}
	return nil
}

func (c *Memcache) findPutTable(entrySize int) *memcacheTable {
	var t *memcacheTable
	i := 0
	// Search a few of the most recent tables for the smallest spot the entry will fit into.
	for e := c.tables.Front(); e != nil && i < 4; e = e.Next() {
		et := e.Value.(*memcacheTable)
		if et.table.FreeSpace() >= entrySize {
			if t == nil || et.table.FreeSpace() < t.table.FreeSpace() {
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
			bt := last.Value.(*memcacheTable)
			bt.free()
			c.tables.Remove(last)
		}
		t = c.allocTable()
		c.tables.PushFront(t)
	}
	err := t.table.Put(key, val)
	if err != nil {
		panic(err)
	}
	for ; c.keys[hash] != nil; hash++ {
	}
	c.keys[hash] = t
}

func (c *Memcache) Put(key, val []byte) {
	hash := farm.Hash64(key)

	c.lock.Lock()
	defer c.lock.Unlock()
	c.putWithHash(key, val, hash)
}

func (c *Memcache) deleteWithHash(key []byte, hash uint64) {
	for ; ; hash++ {
		t, ok := c.keys[hash]
		if !ok {
			break
		} else if t == nil || t.table == nil {
			c.erase(hash)
			continue
		}

		if t.table.Delete(key) {
			c.erase(hash)
			// Since the tables are exclusive, we can stop here.
			break
		}
	}
}

func (c *Memcache) Delete(key []byte) {
	hash := farm.Hash64(key)

	c.lock.Lock()
	defer c.lock.Unlock()
	c.deleteWithHash(key, hash)
}
