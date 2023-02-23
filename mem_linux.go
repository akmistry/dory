package dory

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const (
	memAvailablePrefix = "MemAvailable:"
)

func getMemAvailable() int64 {
	r, err := os.Open("/proc/meminfo")
	if err != nil {
		panic(err)
	}
	defer r.Close()

	memAvailable := int64(0)
	bufr := bufio.NewReader(r)
	for {
		line, err := bufr.ReadString('\n')
		if strings.HasPrefix(line, memAvailablePrefix) {
			remain := strings.TrimSpace(line[len(memAvailablePrefix):])
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

		if err != nil {
			break
		}
	}
	return memAvailable
}

// AvailableMemory returns a MemFunc that cause Memcache to use all available
// memory on the system. The minFree argument is the minimum amount of memory
// that should be kept free. The maxUtilisation is the maximum fraction of
// available memory that should be used.
func AvailableMemory(minFree int64, maxUtilisation float64) MemFunc {
	return func(usage int64) int64 {
		// Include usage in the "available memory" calculation. This is because
		// dory conceptually should only be using available memory. Consider the
		// following scenario, where utilisation is set to 70%:
		//
		// dory usage = 1G, available = 1G
		// Here, total available is 2G, and hence dory should be able to utilise
		// up to 1.4G of memory.
		//
		// The system state changes so that dory usage = 1G, available = 0.1G
		// Now, total availble is 1.1G and hence dory should use up to 0.77G,
		// which is higher than available and should tigger discarding.
		// However, if we use the old calculation which only considered the
		// kernel's "MemAvailable" space, it would calculate dory could use
		// 1.07G, which is not the intended behaviour.
		availableMem := getMemAvailable() + usage - minFree
		return int64(float64(availableMem) * maxUtilisation)
	}
}
