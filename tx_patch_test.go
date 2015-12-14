package lmdb

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestCellKey(tc *testing.T) {
	cellKey := CellKey{"abce", []byte("987654321")}
	serialized := cellKey.Serialize()
	deserialized, err := DeserializeCellKey(serialized)
	ensure.Nil(tc, err)
	ensure.DeepEqual(tc, deserialized, cellKey)
}

func makeTestRWTX(buckets []string, failAtTheEnd bool) func(*ReadWriteTxn) error {
	return func(txn *ReadWriteTxn) error {
		for _, bucket := range buckets {
			txn.Delete(bucket, []byte("foo"))
			txn.Put(bucket, []byte("foo"), []byte("bar"))
			txn.Put(bucket, []byte("foo_x"), []byte("bar_x"))
			txn.Delete(bucket, []byte("foo_x"))
		}

		txn.TransactionalRW(func(txn *ReadWriteTxn) error {
			for _, bucket := range buckets {
				txn.Delete(bucket, []byte("key_a"))
				txn.Put(bucket, []byte("key_a"), []byte("100"))
				txn.Put(bucket, []byte("key_b"), []byte("200"))
			}
			return nil
		})

		txn.TransactionalRW(func(txn *ReadWriteTxn) error {
			for _, bucket := range buckets {
				txn.Put(bucket, []byte("key_bad"), []byte("999"))
			}
			return errors.New("dummy error")
		})

		if failAtTheEnd {
			return errors.New("intentional failure")
		} else {
			return nil
		}
	}
}

func makeTestDb(namePrefix string, buckets []string) (string, *Database) {
	path, err := ioutil.TempDir("", namePrefix)
	if err != nil {
		panic(err)
	}

	db, err := Open(path, buckets)
	if err != nil {
		panic(err)
	}
	return path, db
}

func TestTxnPatch(tc *testing.T) {
	buckets := []string{"bk1, bk2"}

	path1, dbTxn := makeTestDb("dbTxn", buckets)
	defer os.RemoveAll(path1)
	defer dbTxn.Close()

	path2, dbPatch := makeTestDb("dbPatch", buckets)
	defer os.RemoveAll(path2)
	defer dbPatch.Close()

	tx := makeTestRWTX(buckets, false)

	err := dbTxn.TransactionalRW(tx)
	ensure.Nil(tc, err)

	txPatch, err := MakePatch(dbPatch, tx)
	ensure.Nil(tc, err)
	dbPatch.TransactionalRW(func(rwtxn *ReadWriteTxn) error {
		rwtxn.ApplyPatch(txPatch)
		return nil
	})

	ensure.DeepEqual(tc, MakePatchOfDb(dbTxn), MakePatchOfDb(dbPatch))
}

func TestTxnPatch_FailedTx(tc *testing.T) {
	buckets := []string{"bk1, bk2"}

	path1, dbTxn := makeTestDb("dbTxn", buckets)
	defer os.RemoveAll(path1)
	defer dbTxn.Close()

	path2, dbPatch := makeTestDb("dbPatch", buckets)
	defer os.RemoveAll(path2)
	defer dbPatch.Close()

	tx := makeTestRWTX(buckets, true)

	err := dbTxn.TransactionalRW(tx)
	ensure.NotNil(tc, err)

	txPatch, err := MakePatch(dbPatch, tx)
	ensure.True(tc, len(txPatch) == 0)
	ensure.NotNil(tc, err)
	dbPatch.TransactionalRW(func(rwtxn *ReadWriteTxn) error {
		rwtxn.ApplyPatch(txPatch)
		return nil
	})

	ensure.DeepEqual(tc, MakePatchOfDb(dbTxn), MakePatchOfDb(dbPatch))
}
