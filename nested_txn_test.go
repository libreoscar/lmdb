package lmdb

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"
)

const (
	testBucket string = "Bucket0"
)

func SubTxns(txn *ReadWriteTxn, t *testing.T) {
	if n := txn.BucketStat(testBucket).Entries; n != 0 {
		t.Fatal("bucket not empty: ", n)
	}

	// first tx: success
	txn.TransactionalRW(func(txn *ReadWriteTxn) error {
		txn.Put(testBucket, []byte("txn00"), []byte("bar00"))
		txn.Put(testBucket, []byte("txn01"), []byte("bar01"))
		return nil
	})
	if n := txn.BucketStat(testBucket).Entries; n != 2 {
		t.Fatalf("assertion failed. expect 2, got %d", n)
	}

	// second tx: fail
	txn.TransactionalRW(func(txn *ReadWriteTxn) error {
		txn.Put(testBucket, []byte("txn10"), []byte("bar10"))
		txn.Put(testBucket, []byte("txn11"), []byte("bar11"))
		return errors.New("whatever error")
	})
	if n := txn.BucketStat(testBucket).Entries; n != 2 {
		t.Fatalf("assertion failed. expect 2, got %d", n)
	}

	// third tx: success
	txn.TransactionalRW(func(txn *ReadWriteTxn) error {
		txn.Put(testBucket, []byte("txn20"), []byte("bar20"))
		txn.Put(testBucket, []byte("txn21"), []byte("bar21"))
		txn.Put(testBucket, []byte("txn00"), []byte("bar99"))
		return nil
	})
	if n := txn.BucketStat(testBucket).Entries; n != 4 {
		t.Fatalf("assertion failed. expect 4, got %d", n)
	}
}

func TestNestedTxn1(t *testing.T) {
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
		SubTxns(txn, t)
		// commit all
		return nil
	})

	db.TransactionalR(func(txn ReadTxner) {
		if n := txn.BucketStat(testBucket).Entries; n != 4 {
			t.Fatalf("assertion failed. expect 4, got %d", n)
		}

		type kvCases struct {
			key   string
			value string
		}
		cases1 := []kvCases{
			{"txn00", "bar99"},
			{"txn01", "bar01"},
			{"txn20", "bar20"},
			{"txn21", "bar21"},
		}

		for _, c := range cases1 {
			v, b := txn.Get(testBucket, []byte(c.key))
			if !b {
				t.Fatalf("key not found: %s", c.key)
			}
			if string(v) != c.value {
				t.Fatalf("wrong value for %s. expected %s, got %s", c.key, c.value, string(v))
			}
		}

		cases2 := []string{"txn10", "txn11", "txn99"}
		for _, key := range cases2 {
			_, b := txn.Get(testBucket, []byte(key))
			if b {
				t.Fatalf("unexpected key: %s", key)
			}
		}
	})
}

func TestNestedTxn2(t *testing.T) {
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
		SubTxns(txn, t)
		// rollback all
		return errors.New("give me an error!")
	})

	db.TransactionalR(func(txn ReadTxner) {
		if n := txn.BucketStat(testBucket).Entries; n != 0 {
			t.Fatalf("assertion failed. expect 0, got %d", n)
		}

		cases := []string{"txn00", "txn10", "txn11", "txn99"}
		for _, key := range cases {
			_, b := txn.Get(testBucket, []byte(key))
			if b {
				t.Fatalf("unexpected key: %s", key)
			}
		}
	})
}
