package lmdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestDryRunTx(t *testing.T) {
	path, err := ioutil.TempDir("", "lmdb_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(path)

	db, err := Open(path, []string{"bucket"})
	defer db.Close()
	if err != nil {
		panic(err)
	}

	db.TransactionalRW(func(rwtx *ReadWriteTxn) error {
		rwtx.Put("bucket", []byte("key1"), []byte("value1"))
		return nil
	})

	err = DryRunRWTransaction(db, func(rwtx *ReadWriteTxn) error {
		rwtx.Delete("bucket", []byte("key1"))
		rwtx.Put("bucket", []byte("key2"), []byte("value2"))
		return nil
	})
	ensure.Nil(t, err)

	db.TransactionalR(func(rtx ReadTxner) {
		value1, ok1 := rtx.Get("bucket", []byte("key1"))
		ensure.True(t, ok1)
		ensure.DeepEqual(t, value1, []byte("value1"))

		_, ok2 := rtx.Get("bucket", []byte("key2"))
		ensure.False(t, ok2)
	})
}

func TestGetExistingBuckets(t *testing.T) {
	path, err := ioutil.TempDir("", "lmdb_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(path)

	func() {
		db, err := Open(path, []string{"bucket2", "bucket3", "bucket1"})
		defer db.Close()
		if err != nil {
			panic(err)
		}
	}()

	func() {
		db, err := Open(path, nil)
		defer db.Close()

		buckets, err := db.GetExistingBuckets()
		if err != nil {
			panic(err)
		}
		ensure.DeepEqual(t, buckets, []string{"bucket1", "bucket2", "bucket3"})
	}()
}
