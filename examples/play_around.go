package main

import (
	// "errors"
	"fmt"
	"github.com/libreoscar/lmdb"
	"io/ioutil"
	"log"
	// "os"
)

func showBucket(db *lmdb.DB, bucket lmdb.BucketID) {
	db.View(nil, func(txn *lmdb.Txn) {
		itr := txn.Iterate(bucket)
		if itr != nil {
			defer itr.Close()
		} else {
			log.Println("bucket is empty")
			return
		}

		k, v := itr.Get()
		log.Println((string)(k), (string)(v))

		for itr.Next() {
			k, v := itr.Get()
			log.Println((string)(k), (string)(v))
		}
	})
}

func main() {
	log.Println(lmdb.Version())

	path, _ := ioutil.TempDir("", "lmdb_test")
	log.Println(path)
	defer os.RemoveAll(path)

	//------------------------- opening db --------------------------------------

	bucketNames := []string{"b1", "b2"}
	db, buckets, err := lmdb.Open(path, bucketNames)
	// db, buckets, err := lmdb.Open2(path, bucketNames, 100000)
	if db != nil {
		defer db.Close()
	}
	if err != nil {
		panic(err)
	}

	//----------------------- make update ---------------------------------------
	bucket0, bucket1 := buckets[0], buckets[1]
	_, _ = bucket0, bucket1

	db.Update(nil, func(txn *lmdb.Txn) error {
		txn.Put(bucket0, []byte("foo0"), []byte("bar0"))
		txn.Put(bucket0, []byte("foo2"), []byte("bar2"))
		txn.Put(bucket0, []byte("foo1"), []byte("bar1"))

		txn.Put(bucket1, []byte("foo3"), []byte("bar3"))
		txn.Put(bucket1, []byte("foo4"), []byte("bar4"))

		return nil
	})

	// log.Println("-------------------------------")
	// showBucket(db, bucket0)
	// log.Println("-------------------------------")
	// showBucket(db, bucket1)
	// log.Println("-------------------------------")

	//----------------------- stress test ---------------------------------------
	db.Update(nil, func(txn *lmdb.Txn) (err error) {
		for i := 0; i < 10000; i++ {
			txn.Put(bucket0, []byte(fmt.Sprintf("%d", i)), []byte("01234567890abcdef"))
		}
		return nil
	})
}
