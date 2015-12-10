package lmdb

import (
	"os"
	"errors"
	"io/ioutil"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestCellKey(tc *testing.T) {
	cellKey := CellKey{"abce", []byte("987654321")}
	serialized := cellKey.Serialize()
	deserialized := DeserializeCellKey(serialized)
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

func makeTestDb(namePrefix string, buckets []string) *Database {
	path, err := ioutil.TempDir("", namePrefix)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(path)

	db, err := Open(path, buckets)
	if err != nil {
		panic(err)
	}
	return db
}

func TestTxPatch(tc *testing.T) {
	buckets := []string{"bk1, bk2"}

	dbTxn := makeTestDb("dbTxn", buckets)
	defer dbTxn.Close()
	dbPatch := makeTestDb("dbPatch", buckets)
	defer dbPatch.Close()

	tx := makeTestRWTX(buckets, false)

	err := dbTxn.TransactionalRW(tx)
	ensure.Nil(tc, err)

	dbPatch.MakeTxPatch(tx)
	txPatch, err := dbPatch.MakeTxPatch(tx)
	ensure.Nil(tc, err)
	dbPatch.ApplyTxPatch(txPatch)

	ensure.DeepEqual(tc, MakePatchOfDb(dbTxn), MakePatchOfDb(dbPatch))
}

func TestTxPatch_FailedTx(tc *testing.T) {
	buckets := []string{"bk1, bk2"}

	dbTxn := makeTestDb("dbTxn", buckets)
	defer dbTxn.Close()
	dbPatch := makeTestDb("dbPatch", buckets)
	defer dbPatch.Close()

	tx := makeTestRWTX(buckets, true)

	err := dbTxn.TransactionalRW(tx)
	ensure.NotNil(tc, err)

	dbPatch.MakeTxPatch(tx)
	txPatch, err := dbPatch.MakeTxPatch(tx)
	ensure.True(tc, len(txPatch) == 0)
	ensure.NotNil(tc, err)
	dbPatch.ApplyTxPatch(txPatch)

	ensure.DeepEqual(tc, MakePatchOfDb(dbTxn), MakePatchOfDb(dbPatch))
}
