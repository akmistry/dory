package dory

import (
	"syscall"
)

func mmap(size int) ([]byte, error) {
	return syscall.Mmap(0, 0, size, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS|syscall.MAP_POPULATE)
}

func munmap(buf []byte) error {
	return syscall.Munmap(buf)
}
