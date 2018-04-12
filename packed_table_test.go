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

	shortKey = make([]byte, 8)
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

func checkSpace(t *testing.T, pt *PackedTable) {
	t.Helper()
	if pt.LiveSpace()+pt.FreeSpace()+pt.DeletedSpace() != bufferSize {
		t.Errorf("LiveSpace %d + FreeSpace %d + DeletedSpace %d != %d",
			pt.LiveSpace(), pt.FreeSpace(), pt.DeletedSpace(), bufferSize)
	}
}

func TestPackedTable(t *testing.T) {
	buffer := NewPackedTable(make([]byte, bufferSize), 0)

	addedValues := make(map[string]bool)
	addedSize := 0
	for k, v := range values {
		err := buffer.Put([]byte(k), v)
		if err == nil {
			addedValues[k] = true
			addedSize += len(v) + len(k) + 4
		}
		checkSpace(t, buffer)
	}

	t.Logf("Added %d entries, size %d", len(addedValues), addedSize)
	if addedSize < bufferSize-maxValSize-maxKeySize-4 {
		t.Errorf("Added size %d is to small", addedSize)
	}

	keys := buffer.Keys()
	if len(keys) != buffer.NumEntries() {
		t.Errorf("len(keys) %d != # entries %d", len(keys), buffer.NumEntries())
	}
	for _, k := range keys {
		if !buffer.Has(k) {
			t.Errorf("key %v not in table", k)
		}
	}

	// Delete ~20% of added values
	d := 0
	deletedSize := 0
	for k := range addedValues {
		if rand.Float32() > 0.2 {
			continue
		}
		deletedSize += buffer.EntrySize([]byte(k), values[k])
		buffer.Delete([]byte(k))
		delete(addedValues, k)
		d++
		checkSpace(t, buffer)
	}
	t.Logf("Deleted %d values of %d bytes", d, deletedSize)

	keys = buffer.Keys()
	if len(keys) != buffer.NumEntries() {
		t.Errorf("len(keys) %d != # entries %d", len(keys), buffer.NumEntries())
	}
	for _, k := range keys {
		if !addedValues[string(k)] {
			t.Errorf("key %v should not be returned", k)
		}
		if !buffer.Has(k) {
			t.Errorf("key %v not in table", k)
		}
	}

	for i := 0; i < 2; i++ {
		for k, v := range values {
			exists := addedValues[k]

			has := buffer.Has([]byte(k))
			if has != exists {
				t.Errorf("exists %v != has %v", exists, has)
			}

			buf := buffer.Get([]byte(k))
			if exists != (buf != nil) {
				t.Errorf("exists %v != (buf %v != nil)", exists, buf)
			}
			if buf != nil && bytes.Compare(buf, v) != 0 {
				t.Errorf("buffer for %s != expected", k)
			}
		}

		if i == 0 {
			oldFreeSpace := buffer.FreeSpace()
			buffer.GC()
			if buffer.DeletedSpace() != 0 {
				t.Errorf("buffer.DeletedSpace() %d != 0", buffer.DeletedSpace())
			}
			if buffer.FreeSpace() != oldFreeSpace+deletedSize {
				t.Errorf("buffer.FreeSpace() %d != oldFreeSpace %d + deletedSize %d",
					buffer.FreeSpace(), oldFreeSpace, deletedSize)
			}
			checkSpace(t, buffer)
			t.Logf("New free space %d bytes", buffer.FreeSpace())
		}
	}
}

func TestPackedTableOverwrite(t *testing.T) {
	key := []byte("foo")
	val1 := []byte("hello")
	val2 := []byte("world")

	buffer := NewPackedTable(make([]byte, bufferSize), 0)
	if has := buffer.Has(key); has {
		t.Errorf("Unexpected has")
	}
	if err := buffer.Put(key, val1); err != nil {
		t.Errorf("Unexpected put error %v", err)
	}
	if has := buffer.Has(key); !has {
		t.Errorf("Unexpected not has")
	}
	buf := buffer.Get(key)
	if !bytes.Equal(buf, val1) {
		t.Errorf("Unexpected get result %s", string(buf))
	}
	if err := buffer.Put(key, val2); err != nil {
		t.Errorf("Unexpected put error %v", err)
	}
	if has := buffer.Has(key); !has {
		t.Errorf("Unexpected not has")
	}
	buf = buffer.Get(key)
	if !bytes.Equal(buf, val2) {
		t.Errorf("Unexpected get result %s", string(buf))
	}
	buffer.Delete(key)
	if has := buffer.Has(key); has {
		t.Errorf("Unexpected has")
	}
}

func TestPackedTableOverwriteGC(t *testing.T) {
	key := []byte("dkjfhkdjdfhd")
	val := []byte("dfjhgkfdjghkfdj hkdfjhdfkjhgfdkhdfk")

	buffer := NewPackedTable(make([]byte, bufferSize), 0)
	for i := 0; i < 1000000; i++ {
		// Without GC, this will fail after a while.
		if err := buffer.Put(key, val); err != nil {
			t.Fatalf("Unexpected put error %v", err)
		}
		buffer.GC()
		buf := buffer.Get(key)
		if bytes.Compare(buf, val) != 0 {
			t.Errorf("Unexpected get result %s", string(buf))
		}
	}
}

func TestPackedTableOverwriteAutoGC(t *testing.T) {
	key := []byte("dkjfhkdjdfhd")
	val := []byte("dfjhgkfdjghkfdj hkdfjhdfkjhgfdkhdfk")

	buffer := NewPackedTable(make([]byte, bufferSize), bufferSize/2)
	for i := 0; i < 1000000; i++ {
		// Without GC, this will fail after a while.
		if err := buffer.Put(key, val); err != nil {
			t.Fatalf("Unexpected put error %v", err)
		}
		buf := buffer.Get(key)
		if bytes.Compare(buf, val) != 0 {
			t.Errorf("Unexpected get result %s", string(buf))
		}
	}
}

func TestPackedTableReset(t *testing.T) {
	key := []byte("foo")
	val1 := []byte("hello")

	buffer := NewPackedTable(make([]byte, bufferSize), 0)
	if has := buffer.Has(key); has {
		t.Errorf("Unexpected has")
	}
	if err := buffer.Put(key, val1); err != nil {
		t.Errorf("Unexpected put error %v", err)
	}
	if has := buffer.Has(key); !has {
		t.Errorf("Unexpected not has")
	}
	buf := buffer.Get(key)
	if !bytes.Equal(buf, val1) {
		t.Errorf("Unexpected get result %s", string(buf))
	}
	if buffer.NumEntries() != 1 || buffer.NumDeleted() != 0 || buffer.FreeSpace() == bufferSize {
		t.Errorf("Unexpected stats")
	}

	buffer.Reset()
	if has := buffer.Has(key); has {
		t.Errorf("Unexpected has")
	}
	if buffer.NumEntries() != 0 || buffer.NumDeleted() != 0 || buffer.FreeSpace() != bufferSize {
		t.Errorf("Unexpected stats")
	}
}

func BenchmarkPackedTableHas(b *testing.B) {
	buffer := NewPackedTable(make([]byte, bufferSize), 0)
	key := shortKey
	val := []byte("foo")
	buffer.Put(key, val)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Has(key)
	}
}

func BenchmarkPackedTableHasLongKey(b *testing.B) {
	buffer := NewPackedTable(make([]byte, bufferSize), 0)
	key := longKey
	val := []byte("foo")
	buffer.Put(key, val)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Has(key)
	}
}

func BenchmarkPackedTableHasNotExist(b *testing.B) {
	buffer := NewPackedTable(make([]byte, bufferSize), 0)
	key := shortKey

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Has(key)
	}
}

func BenchmarkPackedTableGet(b *testing.B) {
	buffer := NewPackedTable(make([]byte, bufferSize), 0)
	key := shortKey
	val := []byte("foo")
	buffer.Put(key, val)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Get(key)
	}
}

func BenchmarkPackedTableGetNotExist(b *testing.B) {
	buffer := NewPackedTable(make([]byte, bufferSize), 0)
	key := shortKey

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Get(key)
	}
}

const (
	benchKeys   = 4096
	benchKeyLen = 8
)

var (
	benchmarkVal = []byte("foo")
)

func genKeys(keyLen, numKeys int) [][]byte {
	keys := make([][]byte, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		keyBuf := make([]byte, keyLen)
		rand.Read(keyBuf)
		keys = append(keys, keyBuf)
	}
	return keys
}

func benchmarkPackedTablePut_N(b *testing.B, keyLen int) {
	buf := make([]byte, bufferSize)
	benchmarkKeys := genKeys(keyLen, benchKeys)

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	table := NewPackedTable(buf, 0)
	for i < b.N {
		table.Reset()
		for _, k := range benchmarkKeys {
			if i >= b.N {
				break
			}
			err := table.Put(k, benchmarkVal)
			if err != nil {
				// 100% of entries should fit into the table.
				panic(err)
			}
			i++
		}
	}
}

func BenchmarkPackedTablePut_8(b *testing.B) {
	benchmarkPackedTablePut_N(b, 8)
}

func BenchmarkPackedTablePut_64(b *testing.B) {
	benchmarkPackedTablePut_N(b, 64)
}
func BenchmarkPackedTablePut_1024(b *testing.B) {
	benchmarkPackedTablePut_N(b, 1024)
}

func BenchmarkPackedTablePutAndOverwrite(b *testing.B) {
	buf := make([]byte, bufferSize)
	benchmarkKeys := genKeys(benchKeyLen, benchKeys)

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	table := NewPackedTable(buf, 0)
	for ; i < b.N; i += benchKeys {
		table.Reset()
		for _, k := range benchmarkKeys {
			err := table.Put(k, benchmarkVal)
			if err != nil {
				panic(err)
			}
		}
		for _, k := range benchmarkKeys {
			err := table.Put(k, benchmarkVal)
			if err != nil {
				panic(err)
			}
		}
	}
}

func benchmarkPackedTableDelete_N(b *testing.B, keyLen int) {
	buf := make([]byte, bufferSize)
	benchmarkKeys := genKeys(keyLen, benchKeys)

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	table := NewPackedTable(buf, 0)
	for ; i < b.N; i += benchKeys {
		b.StopTimer()
		table.Reset()
		for _, k := range benchmarkKeys {
			err := table.Put(k, benchmarkVal)
			if err != nil {
				panic(err)
			}
		}
		b.StartTimer()
		for _, k := range benchmarkKeys {
			table.Delete(k)
		}
	}
}

func BenchmarkPackedTableDelete_8(b *testing.B) {
	benchmarkPackedTableDelete_N(b, 8)
}

func BenchmarkPackedTableDelete_64(b *testing.B) {
	benchmarkPackedTableDelete_N(b, 64)
}

func BenchmarkPackedTableDelete_1024(b *testing.B) {
	benchmarkPackedTableDelete_N(b, 1024)
}

func benchmarkPackedTableGC_N(b *testing.B, keyLen int) {
	buf := make([]byte, bufferSize)
	benchmarkKeys := genKeys(keyLen, benchKeys)

	var deletedKeys [][]byte
	for _, k := range benchmarkKeys {
		if rand.Float32() > 0.2 {
			continue
		}
		deletedKeys = append(deletedKeys, k)
	}

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	table := NewPackedTable(buf, 0)
	for i < b.N {
		b.StopTimer()
		table.Reset()
		for _, k := range benchmarkKeys {
			err := table.Put(k, benchmarkVal)
			if err != nil {
				panic(err)
			}
		}
		for _, k := range deletedKeys {
			table.Delete(k)
		}
		b.StartTimer()
		table.GC()
		// An "op" is a GC'd key. So the reported ns/op should be interpreted
		// as GCns/key. A GC with 1234 active keys should take 1234*ns/op.
		i += table.NumEntries()
	}
}

func BenchmarkPackedTableGC_8(b *testing.B) {
	benchmarkPackedTableGC_N(b, 8)
}

func BenchmarkPackedTableGC_64(b *testing.B) {
	benchmarkPackedTableGC_N(b, 64)
}

func BenchmarkPackedTableGC_1024(b *testing.B) {
	benchmarkPackedTableGC_N(b, 1024)
}
