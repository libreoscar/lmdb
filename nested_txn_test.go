package lmdb

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"
)

func SubTxns(t *testing.T, db *DB, txn *Txn, bucket BucketID) {
	if n := txn.Stat(bucket).Entries; n != 0 {
		t.Fatal("bucket not empty: ", n)
	}

	// first tx: success
	db.Update(txn, func(txn0 *Txn) error {
		txn0.Put(bucket, []byte("txn00"), []byte("bar00"))
		txn0.Put(bucket, []byte("txn01"), []byte("bar01"))
		return nil
	})
	t.Logf("%+v", txn.Stat(bucket))
	if n := txn.Stat(bucket).Entries; n != 2 {
		t.Fatalf("assertion failed. expect 2, got %d", n)
	}

	// second tx: fail
	db.Update(txn, func(txn1 *Txn) error {
		txn1.Put(bucket, []byte("txn10"), []byte("bar10"))
		txn1.Put(bucket, []byte("txn11"), []byte("bar11"))
		return errors.New("whatever error")
	})
	t.Logf("%+v", txn.Stat(bucket))
	if n := txn.Stat(bucket).Entries; n != 2 {
		t.Fatalf("assertion failed. expect 2, got %d", n)
	}

	// third tx: success
	db.Update(txn, func(txn2 *Txn) error {
		txn2.Put(bucket, []byte("txn20"), []byte("bar20"))
		txn2.Put(bucket, []byte("txn21"), []byte("bar21"))
		txn2.Put(bucket, []byte("txn00"), []byte("bar99"))
		return nil
	})
	t.Logf("%+v", txn.Stat(bucket))
	if n := txn.Stat(bucket).Entries; n != 4 {
		t.Fatalf("assertion failed. expect 4, got %d", n)
	}
}

func TestNestedTxn1(t *testing.T) {
	path, _ := ioutil.TempDir("", "lmdb_test")
	defer os.RemoveAll(path)

	bucketNames := []string{"Bucket1"}
	db, buckets, err := Open(path, bucketNames)
	if db != nil {
		defer db.Close()
	}
	if err != nil {
		panic(err)
	}
	bucket := buckets[0]

	db.Update(nil, func(txn *Txn) error { // outer most txn
		SubTxns(t, db, txn, bucket)
		// commit all
		return nil
	})

	db.View(nil, func(txn *Txn) {
		t.Logf("%+v", txn.Stat(bucket))
		if n := txn.Stat(bucket).Entries; n != 4 {
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
			v, b := txn.Get(bucket, []byte(c.key))
			if !b {
				t.Fatalf("key not found: %s", c.key)
			}
			if string(v) != c.value {
				t.Fatalf("wrong value for %s. expected %s, got %s", c.key, c.value, string(v))
			}
		}

		cases2 := []string{"txn10", "txn11", "txn99"}
		for _, key := range cases2 {
			_, b := txn.Get(bucket, []byte(key))
			if b {
				t.Fatalf("unexpected key: %s", key)
			}
		}
	})
}

func TestNestedTxn2(t *testing.T) {
	path, _ := ioutil.TempDir("", "lmdb_test")
	defer os.RemoveAll(path)

	bucketNames := []string{"Bucket1"}
	db, buckets, err := Open(path, bucketNames)
	if db != nil {
		defer db.Close()
	}
	if err != nil {
		panic(err)
	}
	bucket := buckets[0]

	db.Update(nil, func(txn *Txn) error { // outer most txn
		SubTxns(t, db, txn, bucket)
		// commit all
		return errors.New("give me an error!")
	})

	db.View(nil, func(txn *Txn) {
		t.Logf("%+v", txn.Stat(bucket))
		if n := txn.Stat(bucket).Entries; n != 0 {
			t.Fatalf("assertion failed. expect 0, got %d", n)
		}

		cases := []string{"txn00", "txn10", "txn11", "txn99"}
		for _, key := range cases {
			_, b := txn.Get(bucket, []byte(key))
			if b {
				t.Fatalf("unexpected key: %s", key)
			}
		}
	})
}
