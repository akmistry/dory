package dory

import (
	"math/rand"
	"testing"
)

const (
	keySize = 16
	valSize = 64
)

func BenchmarkMemcacheGet(b *testing.B) {
	const numVal = 100000

	opts := MemcacheOptions{
		TableSize: 128 * 1024,
	}
	c := NewMemcache(opts)

	values := make(map[string]bool, numVal)
	var keyBuf [keySize]byte
	var valBuf [valSize]byte
	for i := 0; i < numVal; i++ {
		rand.Read(keyBuf[:])
		rand.Read(valBuf[:])

		c.Put(keyBuf[:], valBuf[:])
		values[string(keyBuf[:])] = true
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; {
		for k := range values {
			if i > b.N {
				break
			}
			i++

			key := append(keyBuf[:0], k...)
			c.Get(key, valBuf[:0])
		}
	}
}

func BenchmarkMemcachePut(b *testing.B) {
	const numVal = 100000

	opts := MemcacheOptions{
		TableSize: 1024 * 1024,
	}
	c := NewMemcache(opts)

	values := make(map[string][]byte, numVal)
	var keyBuf [keySize]byte
	for i := 0; i < numVal; i++ {
		rand.Read(keyBuf[:])

		val := make([]byte, valSize)
		rand.Read(val)
		values[string(keyBuf[:])] = val
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; {
		for k, v := range values {
			if i > b.N {
				break
			}
			i++

			key := append(keyBuf[:0], k...)
			c.Put(key, v)
		}
	}
}
