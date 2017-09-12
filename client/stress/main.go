package main

import (
	"bytes"
	"context"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/akmistry/dory/client"
)

const (
	stressThreads = 2

	minKeySize = 16
	maxKeySize = 32
	minValSize = 1
	maxValSize = 16 * 1024

	liveValues    = 10000
	cycledValues  = 1000
	deletedValues = 1000
)

func generateValues(values map[string][]byte, count int) {
	for i := 0; i < count; i++ {
		keyLen := rand.Intn(maxKeySize-minKeySize) + minKeySize
		valLen := rand.Intn(maxValSize-minValSize) + minValSize

		keyBuf := make([]byte, keyLen)
		rand.Read(keyBuf)
		valBuf := make([]byte, valLen)
		rand.Read(valBuf)
		values[string(keyBuf)] = valBuf
	}
}

func replaceValues(values map[string][]byte, count int) {
	i := 0
	for k := range values {
		if i > count {
			return
		}
		valLen := rand.Intn(maxValSize-minValSize) + minValSize
		buf := make([]byte, valLen)
		rand.Read(buf)
		values[k] = buf
		i++
	}
}

func deleteValues(values map[string][]byte, count int) {
	i := 0
	for k := range values {
		if i > count {
			return
		}
		delete(values, k)
		i++
	}
}

func stressFunc(ctx context.Context, c *client.Client) {
	values := make(map[string][]byte)
	generateValues(values, liveValues)
	for ctx.Err() == nil {
		start := time.Now()
		dataSize := 0
		for k, v := range values {
			dataSize += len(k) + len(v)
			c.Put(ctx, []byte(k), v)
		}
		log.Printf("Data size %d", dataSize)
		log.Printf("Put time: %0.2f sec", time.Since(start).Seconds())

		i := 0
		for k := range values {
			if i >= deletedValues {
				break
			}
			c.Delete(ctx, []byte(k))
			i++
		}

		getBuf := make([]byte, 0, maxValSize)
		start = time.Now()
		missing := 0
		for k, v := range values {
			buf, _ := c.Get(ctx, []byte(k), getBuf)
			if buf == nil {
				missing++
			} else if bytes.Compare(v, buf) != 0 {
				panic("bytes not equal")
			}
		}
		log.Printf("Get time: %0.2f sec, missing %d", time.Since(start).Seconds(), missing)

		deleteValues(values, deletedValues)
		replaceValues(values, cycledValues)
		generateValues(values, deletedValues)
	}
}

func main() {
	go func() {
		err := http.ListenAndServe("127.0.0.1:6666", nil)
		if err != nil {
			panic(err)
		}
	}()

	c := client.NewClient("127.0.0.1:19513", 0)

	var wg sync.WaitGroup
	ctx, cf := context.WithCancel(context.Background())
	for i := 0; i < stressThreads; i++ {
		wg.Add(1)
		go func() {
			stressFunc(ctx, c)
			wg.Done()
		}()
	}

	time.Sleep(time.Minute * 60)
	cf()
	wg.Wait()
}
