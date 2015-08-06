package main

import (
	"fmt"
	mdb "github.com/szferi/gomdb"
	"io/ioutil"
	"os"
)

func CreateEnv(path string, mapSize uint64) *mdb.Env {
	env, _ := mdb.NewEnv()  // create env
	env.SetMapSize(mapSize) // settings
	env.Open(path, 0, 0664) // path, flags, file_mode
	return env
}

func main() {
	path, _ := ioutil.TempDir("", "mdb_test")
	fmt.Println(path)
	defer os.RemoveAll(path)

	env := CreateEnv(path, 1<<20)
	defer env.Close() // cleaning up

	txn, _ := env.BeginTxn(nil, 0) // parent, flags

	dbi, _ := txn.DBIOpen(nil, 0) // name, flag(CREATE), mdb_env_set_maxdbs for named db
	defer env.DBIClose(dbi)

	txn.Commit()

	// write some data
	txn, _ = env.BeginTxn(nil, 0)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("Key-%d", i)
		val := fmt.Sprintf("Val-%d", i)
		txn.Put(dbi, []byte(key), []byte(val), 0)
	}
	txn.Commit()

	// inspect the database
	fmt.Println(env.Stat())

	// scan the database
	txn, _ = env.BeginTxn(nil, mdb.RDONLY)
	defer txn.Abort()
	cursor, _ := txn.CursorOpen(dbi)
	defer cursor.Close()
	for {
		bkey, bval, err := cursor.Get(nil, nil, mdb.NEXT)
		if err == mdb.NotFound {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s: %s\n", bkey, bval)
	}

	// random access
	bval, _ := txn.Get(dbi, []byte("Key-3"))
	fmt.Println(string(bval))
}
