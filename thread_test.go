package lmdb

import (
	"github.com/facebookgo/ensure"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestThread(t *testing.T) {
	path, err := ioutil.TempDir("", "lmdb_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(path)

	bucketNames := []string{BucketName}
	db, err := Open(path, bucketNames)
	defer db.Close()
	if err != nil {
		panic(err)
	}

	db.TransactionalRW(func(txn *ReadWriteTxn) error {
		txn.Put(BucketName, []byte("foo"), []byte("bar"))
		return nil
	})

	db.TransactionalRW(func(txn *ReadWriteTxn) error {
		txn.Put(BucketName, []byte("foo2"), []byte("bar2"))

		db.TransactionalR(func(txnR ReadTxner) {
			val, exist := txnR.Get(BucketName, []byte("foo"))
			ensure.True(t, exist)
			ensure.DeepEqual(t, val, []byte("bar"))

			_, exist2 := txnR.Get(BucketName, []byte("foo2"))
			ensure.False(t, exist2)
		})
		return nil
	})
}

func TestThread2(t *testing.T) {
	path, err := ioutil.TempDir("", "lmdb_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(path)

	bucketNames := []string{BucketName}
	db, err := Open(path, bucketNames)
	defer db.Close()
	if err != nil {
		panic(err)
	}

	go db.TransactionalRW(func(txn *ReadWriteTxn) error {
		t.Log("first RW txn begins")
		txn.Put(BucketName, []byte("foo"), []byte("bar"))
		time.Sleep(1 * time.Second)
		_, exist := txn.Get(BucketName, []byte("foo1"))
		ensure.False(t, exist)
		t.Log("first RW txn ends")
		return nil
	})

	time.Sleep(100 * time.Millisecond)
	go db.TransactionalRW(func(txn *ReadWriteTxn) error {
		t.Log("second RW txn begins")
		txn.Put(BucketName, []byte("foo1"), []byte("bar1"))
		t.Log("second RW txn ends")
		return nil
	})

	time.Sleep(2 * time.Second)
}
