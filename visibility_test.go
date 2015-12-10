package lmdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestVisibility1(t *testing.T) {
	path, err := ioutil.TempDir("", "lmdb_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(path)

	bucketNames := []string{testBucket}
	db, err := Open(path, bucketNames)
	defer db.Close()
	if err != nil {
		panic(err)
	}

	db.TransactionalRW(func(txn *ReadWriteTxn) error {
		if n := txn.BucketStat(testBucket).Entries; n != 0 {
			t.Fatal("bucket not empty: ", n)
		}

		txn.Put(testBucket, []byte("txn0"), []byte("bar0"))
		{
			val, exist := txn.GetNoCopy(testBucket, []byte("txn0"))
			ensure.True(t, exist)
			ensure.DeepEqual(t, val, []byte("bar0"))
		}

		// first tx
		txn.TransactionalRW(func(txn *ReadWriteTxn) error {
			{ // parent's change should be visiable to child
				val, exist := txn.GetNoCopy(testBucket, []byte("txn0"))
				ensure.True(t, exist)
				ensure.DeepEqual(t, val, []byte("bar0"))
			}

			txn.Put(testBucket, []byte("txn00"), []byte("bar00"))
			{
				val, exist := txn.GetNoCopy(testBucket, []byte("txn00"))
				ensure.True(t, exist)
				ensure.DeepEqual(t, val, []byte("bar00"))
			}
			return nil
		})

		// second tx
		txn.TransactionalRW(func(txn *ReadWriteTxn) error {
			// first child's change should be visiable to the following child
			val, exist := txn.GetNoCopy(testBucket, []byte("txn00"))
			ensure.True(t, exist)
			ensure.DeepEqual(t, val, []byte("bar00"))
			return nil
		})

		// commit all
		return nil
	})
}
