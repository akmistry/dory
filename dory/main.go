package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"golang.org/x/net/http2"

	"github.com/akmistry/dory"
	"github.com/akmistry/dory/server"
)

const (
	megabyte         = 1024 * 1024
	defaultTableSize = 4 * megabyte
)

var (
	listenAddr      = flag.String("listen-addr", "0.0.0.0:19513", "Address/port to listen on")
	redisListenAddr = flag.String("redis-listen-addr", "", "Address/port for redis server to listen on")

	minAvailableMb        = flag.Int("min-available-mb", 512, "Minimum available memory, in MiB")
	maxKeySize            = flag.Int("max-key-size", 1024, "Max key size in bytes")
	maxValSize            = flag.Int("max-val-size", 1024*1024, "Max value size in bytes")
	oomAdj                = flag.Bool("oom-adj", true, "Adjust OOM score so that we're killed first")
	maxConcurrentRequests = flag.Int(
		"max-concurrent-requests", 64, "Maximum number of concurrent get/put requests")
	constCacheSizeMb = flag.Int("const-cache-size-mb", 0,
		"Constant cache size, in MiB. Default 0 = use all available memory up to --min-available-mb")

	promPort  = flag.Int("prom-port", 0, "Port to export prometheus metrics")
	pprofAddr = flag.String("pprof-addr", "", "Address/port to serve pprof")
)

func main() {
	runtime.SetBlockProfileRate(100)
	runtime.SetMutexProfileFraction(100)

	flag.Parse()

	if *promPort > 0 {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *promPort), mux)
			if err != nil {
				panic(err)
			}
		}()
	}

	if *pprofAddr != "" {
		go func() {
			err := http.ListenAndServe(*pprofAddr, nil)
			if err != nil {
				panic(err)
			}
		}()
	}

	if *oomAdj {
		err := ioutil.WriteFile("/proc/self/oom_score_adj", []byte("1000"), 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to adjust OOM score: %v", err)
		}
	}

	cacheOpts := dory.MemcacheOptions{
		MemoryFunction: dory.AvailableMemory(int64(*minAvailableMb)*megabyte, 1.0),
		MaxKeySize:     *maxKeySize,
		MaxValSize:     *maxValSize,
	}
	if *constCacheSizeMb != 0 {
		cacheOpts.MemoryFunction = dory.ConstantMemory(int64(*constCacheSizeMb) * megabyte)
	}
	cache := dory.NewMemcache(cacheOpts)
	handler := server.NewHandler(cache, *maxConcurrentRequests)

	l, err := net.Listen("tcp4", *listenAddr)
	if err != nil {
		panic(err)
	}

	if *redisListenAddr != "" {
		go func() {
			redisL, err := net.Listen("tcp4", *redisListenAddr)
			if err != nil {
				panic(err)
			}
			redisServer := server.NewRedisServer(cache)

			for {
				c, err := redisL.Accept()
				if err != nil {
					panic(err)
				}
				go func() {
					defer c.Close()
					err := redisServer.Serve(c)
					if err != nil && !strings.Contains(err.Error(), "connection reset by peer") {
						log.Print("Redis server error:", err)
					}
				}()
			}
		}()
	}

	h2s := &http2.Server{
		// TODO: Flag configurable.
		MaxConcurrentStreams: 8,
		IdleTimeout:          time.Minute,
		NewWriteScheduler:    server.NewRoundRobinScheduler,
	}
	serverConnOpts := &http2.ServeConnOpts{Handler: handler}
	for {
		c, err := l.Accept()
		if err != nil {
			panic(err)
		}
		go func() {
			defer c.Close()
			h2s.ServeConn(c, serverConnOpts)
		}()
	}
}
