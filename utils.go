package lmdb

import (
	"github.com/szferi/gomdb"
	"syscall"
)

// nil error -> mdb.SUCCESS (0)
func errorToNum(err error) int {
	if err == nil {
		return mdb.SUCCESS
	}
	if v, ok := err.(mdb.Errno); ok {
		return int(v)
	}
	if v, ok := err.(syscall.Errno); ok {
		return int(v)
	}
	panic("Invalid input")
}
