package lmdb

import (
	"github.com/szferi/gomdb"
	"testing"
)

func TestErrorToNum(t *testing.T) {
	cases := []struct {
		err error
		v   int
	}{
		{nil, (int)(mdb.SUCCESS)},
		{mdb.Errno(mdb.NotFound), (int)(mdb.NotFound)},
		{mdb.Errno(123), 123},
	}
	for _, e := range cases {
		v := errorToNum(e.err)
		if v != e.v {
			t.Errorf("errorToNum failed, expect %d, got %d", e.v, v)
		}
	}
}
