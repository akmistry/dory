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

func AvalableMemory(minFree int64, maxUtilisation float64) MemFunc {
	return func(usage int64) int64 {
		return int64(float64(getMemAvailable())*maxUtilisation) + usage - minFree
	}
}
