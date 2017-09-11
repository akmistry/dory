package dory

import (
	"bytes"
	"math/rand"
	"testing"
)

const (
	minKeySize = 1
	maxKeySize = 32
	minValSize = 1
	maxValSize = 128 * 1024

	bufferSize = 16 * 1024 * 1024
)

var (
	values = make(map[string][]byte)

	shortKey = make([]byte, 32)
	longKey  = make([]byte, 1024)
)

func init() {
	const generatedValues = 1024

	for i := 0; i < generatedValues; i++ {
		keyLen := rand.Intn(maxKeySize-minKeySize) + minKeySize
		valLen := rand.Intn(maxValSize-minValSize) + minValSize

		keyBuf := make([]byte, keyLen)
		rand.Read(keyBuf)
		valBuf := make([]byte, valLen)
		rand.Read(valBuf)
		values[string(keyBuf)] = valBuf
	}

	rand.Read(shortKey)
	rand.Read(longKey)
}

func TestPackedTable(t *testing.T) {
	buffer := NewPackedTable(make([]byte, bufferSize))

	addedValues := make(map[string]bool)
	addedSize := 0
	for k, v := range values {
		err := buffer.Put([]byte(k), v)
		if err == nil {
			addedValues[k] = true
			addedSize += len(v) + len(k) + 4
		}
	}

	t.Logf("Added %d entries, size %d", len(addedValues), addedSize)
	if addedSize < bufferSize-maxValSize-maxKeySize-4 {
		t.Errorf("Added size %d is to small", addedSize)
	}

	// Delete ~20% of added values
	i := 0
	d := len(addedValues) / 5
	for k := range addedValues {
		if i >= d {
			break
		}
		buffer.Delete([]byte(k))
		delete(addedValues, k)
	}
	t.Logf("Deleted %d values", d)

	for k, v := range values {
		exists := addedValues[k]

		has := buffer.Has([]byte(k))
		if has != exists {
			t.Errorf("exists %v != has %v", exists, has)
		}

		buf := buffer.Get([]byte(k), nil)
		if exists != (buf != nil) {
			t.Errorf("exists %v != (buf %v != nil)", exists, buf)
		}
		if buf != nil && bytes.Compare(buf, v) != 0 {
			t.Errorf("buffer for %s != expected", k)
		}
	}
}

func TestPackedTableOverwrite(t *testing.T) {
	key := "foo"
	val1 := "hello"
	val2 := "world"

	buffer := NewPackedTable(make([]byte, bufferSize))
	if has := buffer.Has([]byte(key)); has {
		t.Errorf("Unexpected has")
	}
	if err := buffer.Put([]byte(key), []byte(val1)); err != nil {
		t.Errorf("Unexpected put error %v", err)
	}
	if has := buffer.Has([]byte(key)); !has {
		t.Errorf("Unexpected not has")
	}
	buf := buffer.Get([]byte(key), nil)
	if string(buf) != val1 {
		t.Errorf("Unexpected get result %s", string(buf))
	}
	if err := buffer.Put([]byte(key), []byte(val2)); err != nil {
		t.Errorf("Unexpected put error %v", err)
	}
	if has := buffer.Has([]byte(key)); !has {
		t.Errorf("Unexpected not has")
	}
	buf = buffer.Get([]byte(key), nil)
	if string(buf) != val2 {
		t.Errorf("Unexpected get result %s", string(buf))
	}
	buffer.Delete([]byte(key))
	if has := buffer.Has([]byte(key)); has {
		t.Errorf("Unexpected has")
	}
}

func BenchmarkPackedTableHas(b *testing.B) {
	buffer := NewPackedTable(make([]byte, bufferSize))
	key := shortKey
	val := make([]byte, 12345)
	rand.Read(val)
	buffer.Put(key, val)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Has(key)
	}
}

func BenchmarkPackedTableHasLongKey(b *testing.B) {
	buffer := NewPackedTable(make([]byte, bufferSize))
	key := longKey
	val := make([]byte, 12345)
	rand.Read(val)
	buffer.Put(key, val)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Has(key)
	}
}

func BenchmarkPackedTableHasNotExist(b *testing.B) {
	buffer := NewPackedTable(make([]byte, bufferSize))
	key := shortKey

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Has(key)
	}
}

func BenchmarkPackedTableGet(b *testing.B) {
	buffer := NewPackedTable(make([]byte, bufferSize))
	key := shortKey
	val := make([]byte, 12345)
	rand.Read(val)
	buffer.Put(key, val)

	outBuf := make([]byte, 0, len(val))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Get(key, outBuf)
	}
}

func BenchmarkPackedTableGetNotExist(b *testing.B) {
	buffer := NewPackedTable(make([]byte, bufferSize))
	key := shortKey

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Get(key, nil)
	}
}

type pair struct {
	k, v []byte
}

func BenchmarkPackedTablePut(b *testing.B) {
	buf := make([]byte, bufferSize)

	pairs := make([]pair, 0, len(values))
	for k, v := range values {
		pairs = append(pairs, pair{[]byte(k), v})
	}

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for i < b.N {
		buffer := NewPackedTable(buf)
		for _, p := range pairs {
			if i >= b.N {
				break
			}
			buffer.Put(p.k, p.v)
			i++
		}
	}
}

func BenchmarkPackedTablePutAndDelete(b *testing.B) {
	buf := make([]byte, bufferSize)

	pairs := make([]pair, 0, len(values))
	for k, v := range values {
		pairs = append(pairs, pair{[]byte(k), v})
	}

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for i < b.N {
		buffer := NewPackedTable(buf)
		for _, p := range pairs {
			if i >= b.N {
				break
			}
			buffer.Put(p.k, p.v)
			i++
		}
		for _, p := range pairs {
			buffer.Delete(p.k)
		}
	}
}

func BenchmarkPackedTablePutAndOverwrite(b *testing.B) {
	buf := make([]byte, bufferSize)

	pairs := make([]pair, 0, len(values))
	for k, v := range values {
		pairs = append(pairs, pair{[]byte(k), v})
	}

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for i < b.N {
		buffer := NewPackedTable(buf)
		for _, p := range pairs {
			if i >= b.N {
				break
			}
			buffer.Put(p.k, p.v)
			i++
		}
		for _, p := range pairs {
			buffer.Put(p.k, p.v)
		}
	}
}
