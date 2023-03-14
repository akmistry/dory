package dory

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	keySize = 16
	valSize = 64
)

func putString(c *Memcache, key, val string) {
	c.Put([]byte(key), []byte(val))
}

func getString(c *Memcache, key string) string {
	return string(c.Get([]byte(key), nil))
}

func hasString(c *Memcache, key string) bool {
	return c.Has([]byte(key))
}

func deleteString(c *Memcache, key string) {
	c.Delete([]byte(key))
}

func TestMemcache_HashCollisions(t *testing.T) {
	opts := MemcacheOptions{
		HashFunction: func(b []byte) uint64 {
			// Designated, sufficiently random number
			return 7
		},
	}
	c := NewMemcache(opts)

	assert.False(t, hasString(c, "foo"))
	assert.False(t, hasString(c, "bar"))
	assert.False(t, hasString(c, "baz"))

	putString(c, "foo", "11")
	putString(c, "bar", "22")
	assert.True(t, hasString(c, "foo"))
	assert.True(t, hasString(c, "bar"))
	assert.False(t, hasString(c, "baz"))
	assert.Equal(t, "11", getString(c, "foo"))
	assert.Equal(t, "22", getString(c, "bar"))
	assert.Equal(t, "", getString(c, "baz"))

	deleteString(c, "foo")
	assert.False(t, hasString(c, "foo"))
	assert.True(t, hasString(c, "bar"))
	assert.False(t, hasString(c, "baz"))
	assert.Equal(t, "", getString(c, "foo"))
	assert.Equal(t, "22", getString(c, "bar"))
	assert.Equal(t, "", getString(c, "baz"))

	putString(c, "baz", "33")
	putString(c, "foo", "44")
	assert.True(t, hasString(c, "foo"))
	assert.True(t, hasString(c, "bar"))
	assert.True(t, hasString(c, "baz"))
	assert.Equal(t, "44", getString(c, "foo"))
	assert.Equal(t, "22", getString(c, "bar"))
	assert.Equal(t, "33", getString(c, "baz"))

	deleteString(c, "baz")
	deleteString(c, "foo")
	assert.False(t, hasString(c, "foo"))
	assert.True(t, hasString(c, "bar"))
	assert.False(t, hasString(c, "baz"))
	assert.Equal(t, "", getString(c, "foo"))
	assert.Equal(t, "22", getString(c, "bar"))
	assert.Equal(t, "", getString(c, "baz"))
}

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
