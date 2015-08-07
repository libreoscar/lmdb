package main

import (
	"github.com/libreoscar/lmdb"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	log.Println(lmdb.Version())

	path, _ := ioutil.TempDir("", "mdb_test")
	log.Println(path)
	defer os.RemoveAll(path)

	db, err := lmdb.Open2(path, 0, 0) // TODO: change parameter
	defer db.Close()
	if err != nil {
		log.Fatalln(err)
	}

	// env := CreateEnv(path, 1<<20)
	// defer env.Close() // cleaning up

	// txn, _ := env.BeginTxn(nil, 0) // parent, flags

	// dbi, _ := txn.DBIOpen(nil, 0) // name, flag(CREATE), mdb_env_set_maxdbs for named db
	// defer env.DBIClose(dbi)

	// txn.Commit()

	// // write some data
	// txn, _ = env.BeginTxn(nil, 0)
	// for i := 0; i < 5; i++ {
	// 	key := fmt.Sprintf("Key-%d", i)
	// 	val := fmt.Sprintf("Val-%d", i)
	// 	txn.Put(dbi, []byte(key), []byte(val), 0)
	// }
	// txn.Commit()

	// // inspect the database
	// fmt.Println(env.Stat())

	// // scan the database
	// txn, _ = env.BeginTxn(nil, mdb.RDONLY)
	// defer txn.Abort()
	// cursor, _ := txn.CursorOpen(dbi)
	// defer cursor.Close()
	// for {
	// 	bkey, bval, err := cursor.Get(nil, nil, mdb.NEXT)
	// 	if err == mdb.NotFound {
	// 		break
	// 	}
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Printf("%s: %s\n", bkey, bval)
	// }

	// // random access
	// bval, _ := txn.Get(dbi, []byte("Key-3"))
	// fmt.Println(string(bval))
}
