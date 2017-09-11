package dory

import (
	"os"
)

var (
	debugLog bool
)

func init() {
	if os.Getenv("DORY_DEBUG") == "1" {
		debugLog = true
	}
}
