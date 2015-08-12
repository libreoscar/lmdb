package lmdb

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"
)

const (
	BucketName string = "Bucket0"
)

func SubTxns(ctx *Context, t *testing.T) {
	if n := ctx.TxStat(BucketName).Entries; n != 0 {
		t.Fatal("bucket not empty: ", n)
	}

	// first tx: success
	ctx.Transactional(true, func(ctx *Context) error {
		ctx.Put(BucketName, []byte("txn00"), []byte("bar00"))
		ctx.Put(BucketName, []byte("txn01"), []byte("bar01"))
		return nil
	})
	if n := ctx.TxStat(BucketName).Entries; n != 2 {
		t.Fatalf("assertion failed. expect 2, got %d", n)
	}

	// second tx: fail
	ctx.Transactional(true, func(ctx *Context) error {
		ctx.Put(BucketName, []byte("txn10"), []byte("bar10"))
		ctx.Put(BucketName, []byte("txn11"), []byte("bar11"))
		return errors.New("whatever error")
	})
	if n := ctx.TxStat(BucketName).Entries; n != 2 {
		t.Fatalf("assertion failed. expect 2, got %d", n)
	}

	// third tx: success
	ctx.Transactional(true, func(ctx *Context) error {
		ctx.Put(BucketName, []byte("txn20"), []byte("bar20"))
		ctx.Put(BucketName, []byte("txn21"), []byte("bar21"))
		ctx.Put(BucketName, []byte("txn00"), []byte("bar99"))
		return nil
	})
	if n := ctx.TxStat(BucketName).Entries; n != 4 {
		t.Fatalf("assertion failed. expect 4, got %d", n)
	}
}

func TestNestedTxn1(t *testing.T) {
	path, _ := ioutil.TempDir("", "lmdb_test")
	defer os.RemoveAll(path)

	bucketNames := []string{BucketName}
	ctx, err := Open(path, bucketNames)
	defer ctx.Close()

	if err != nil {
		panic(err)
	}

	ctx.Transactional(true, func(ctx *Context) error {
		SubTxns(ctx, t)
		// commit all
		return nil
	})

	ctx.Transactional(false, func(ctx *Context) error {
		if n := ctx.TxStat(BucketName).Entries; n != 4 {
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
			v, b := ctx.Get(BucketName, []byte(c.key))
			if !b {
				t.Fatalf("key not found: %s", c.key)
			}
			if string(v) != c.value {
				t.Fatalf("wrong value for %s. expected %s, got %s", c.key, c.value, string(v))
			}
		}

		cases2 := []string{"txn10", "txn11", "txn99"}
		for _, key := range cases2 {
			_, b := ctx.Get(BucketName, []byte(key))
			if b {
				t.Fatalf("unexpected key: %s", key)
			}
		}
		return nil
	})
}

func TestNestedTxn2(t *testing.T) {
	path, _ := ioutil.TempDir("", "lmdb_test")
	defer os.RemoveAll(path)

	bucketNames := []string{BucketName}
	ctx, err := Open(path, bucketNames)
	defer ctx.Close()

	if err != nil {
		panic(err)
	}

	ctx.Transactional(true, func(ctx *Context) error {
		SubTxns(ctx, t)
		// rollback all
		return errors.New("give me an error!")
	})

	ctx.Transactional(false, func(ctx *Context) error {
		if n := ctx.TxStat(BucketName).Entries; n != 0 {
			t.Fatalf("assertion failed. expect 0, got %d", n)
		}

		cases := []string{"txn00", "txn10", "txn11", "txn99"}
		for _, key := range cases {
			_, b := ctx.Get(BucketName, []byte(key))
			if b {
				t.Fatalf("unexpected key: %s", key)
			}
		}
		return nil
	})
}
