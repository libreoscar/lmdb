package lmdb

import (
	"github.com/facebookgo/ensure"
	"io/ioutil"
	"os"
	"testing"
)

func TestVisibility1(t *testing.T) {
	path, _ := ioutil.TempDir("", "lmdb_test")
	defer os.RemoveAll(path)

	bucketNames := []string{BucketName}
	db, err := Open(path, bucketNames)
	defer db.Close()
	if err != nil {
		panic(err)
	}

	db.TransactionalRW(func(txn *ReadWriteTxn) error {
		if n := txn.BucketStat(BucketName).Entries; n != 0 {
			t.Fatal("bucket not empty: ", n)
		}

		txn.Put(BucketName, []byte("txn0"), []byte("bar0"))
		{
			val, exist := txn.GetNoCopy(BucketName, []byte("txn0"))
			ensure.True(t, exist)
			ensure.DeepEqual(t, val, []byte("bar0"))
		}

		// first tx
		txn.TransactionalRW(func(txn *ReadWriteTxn) error {
			{ // parent's change should be visiable to child
				val, exist := txn.GetNoCopy(BucketName, []byte("txn0"))
				ensure.True(t, exist)
				ensure.DeepEqual(t, val, []byte("bar0"))
			}

			txn.Put(BucketName, []byte("txn00"), []byte("bar00"))
			{
				val, exist := txn.GetNoCopy(BucketName, []byte("txn00"))
				ensure.True(t, exist)
				ensure.DeepEqual(t, val, []byte("bar00"))
			}
			return nil
		})

		// second tx
		txn.TransactionalRW(func(txn *ReadWriteTxn) error {
			// first child's change should be visiable to the following child
			val, exist := txn.GetNoCopy(BucketName, []byte("txn00"))
			ensure.True(t, exist)
			ensure.DeepEqual(t, val, []byte("bar00"))
			return nil
		})

		// commit all
		return nil
	})
}
