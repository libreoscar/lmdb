package main

import (
	// "errors"
	"github.com/libreoscar/lmdb"
	"io/ioutil"
	"log"
	"os"
	// "time"
)

// TODO: test nested tx
// TODO: multi threaded

func showBucket(db *lmdb.DB, bucket lmdb.BucketID) {
	db.View(nil, func(txn *lmdb.Txn) {
		// itr := txn.BucketBegin(bucket)
		// defer itr.Close()

		// for key, value, more := itr.Next(); more; {
		// 	log.Println(key, value)
		// }
		val, b := txn.Get(bucket, []byte("foo0"))
		if b {
			log.Println("foo0", string(val))
		} else {
			log.Println("foo0: None")
		}

		val, b = txn.Get(bucket, []byte("foo1"))
		if b {
			log.Println("foo1", string(val))
		} else {
			log.Println("foo1: None")
		}

		val, b = txn.Get(bucket, []byte("foo2"))
		if b {
			log.Println("foo2", string(val))
		} else {
			log.Println("foo2: None")
		}

		val, b = txn.Get(bucket, []byte("foo3"))
		if b {
			log.Println("foo3", string(val))
		} else {
			log.Println("foo3: None")
		}

		val, b = txn.Get(bucket, []byte("foo4"))
		if b {
			log.Println("foo4", string(val))
		} else {
			log.Println("foo4: None")
		}

		val, b = txn.Get(bucket, []byte("foo5"))
		if b {
			log.Println("foo5", string(val))
		} else {
			log.Println("foo5: None")
		}
	})
}

func main() {
	log.Println(lmdb.Version())

	path, _ := ioutil.TempDir("", "mdb_test")
	log.Println(path)
	defer os.RemoveAll(path)

	//------------------------- opening db --------------------------------------

	bucketNames := []string{"b1", "b2"}
	db, buckets, err := lmdb.Open(path, bucketNames) // TODO: change parameter
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	if err != nil {
		panic(err)
	}

	// log.Println(buckets)
	// log.Printf("%+v", db.Stat())
	// log.Printf("%+v", db.Info())

	//----------------------- make update ---------------------------------------
	bucket0, bucket1 := buckets[0], buckets[1]
	db.Update(nil, func(txn *lmdb.Txn) error {
		txn.Put(bucket0, []byte("foo0"), []byte("bar0"))
		txn.Put(bucket0, []byte("foo1"), []byte("bar1"))
		txn.Put(bucket0, []byte("foo2"), []byte("bar2"))

		txn.Put(bucket1, []byte("foo3"), []byte("bar3"))
		txn.Put(bucket1, []byte("foo4"), []byte("bar4"))
		// log.Printf("%+v", txn.Stat(bucket0))
		// log.Printf("%+v", txn.Stat(bucket1))

		return nil
	})

	// showBucket(db, bucket0)
	// showBucket(db, bucket1)
}
