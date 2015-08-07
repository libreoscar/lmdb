package lmdb

import (
	"github.com/szferi/gomdb"
)

// Thread Safety
// 1) NOTLS mode is used exclusively, which allows read txns to freely migrate across
//    threads and for a single thread to maintain multiple read txns. This enables mostly
//    care-free use of read txns, for example when using gevent.
// 2) Most objects can be safely called by a single caller from a single thread, and usually it
//    only makes sense to have a single caller, except in the case of DB.
// 3) Most DB methods are thread-safe, and may be called concurrently, except for DB.close().
// 4) A write txn (DB.Update) may only be used from the thread it was created on.
// 5) A read-only txn (DB.View) can move across threads, but it cannot be used concurrently from
//    multiple threads.
// 6) Iterator is not thread-safe, but it does not make sense to use it on any thread except the
//    thread that currently owns its associated txn.
//
//-------------------------------------------------------------------------------------------------
//
// Best practice:
// 1) Create/Drop buckets in the beginning of the program, before any other txns; do not drop or
//    open buckets when the program runs in parallel afterwards.
// 2) Use iterators only in the txn that they are created; close them before txn ends.
// 3) Close all read/write txns before DB.Close()

const (
	// Named databases + main and free DB are 30 + 2 == 32
	MAX_BUCKET_SIZE_DEFAULT uint = 30

	// On 64-bit there is no penalty for making this huge
	MAP_SIZE_DEFAULT uint64 = 1024 * 1024 * 1024 * 1024 // 1TB
)

type DB struct {
	env *mdb.Env
}

type Stat struct {
	mdb.Stat
}

type Info struct {
	mdb.Info
}

//--------------------------------- DB ------------------------------------------------------------

func Version() string {
	return mdb.Version()
}

func Open(path string) (*DB, error) {
	return Open2(path, MAX_BUCKET_SIZE_DEFAULT, MAP_SIZE_DEFAULT)
}

func Open2(path string, maxBuckets uint, maxMapSize uint64) (*DB, error) {
	env, err := mdb.NewEnv()
	if err != nil { // Possible errors: ENOMEM (out of memory)
		panic(err)
	}

	err = env.SetMapSize(maxMapSize)
	if err != nil { // Possible errors: EINVAL, other system criticle errors
		panic(err)
	}

	// http://www.openldap.org/lists/openldap-technical/201305/msg00176.html
	err = env.SetMaxDBs((mdb.DBI)(maxBuckets))
	if err != nil { // Possible errors: EINVAL
		panic(err)
	}

	err = env.Open(path, 0, 0664) // NOTLS is enforced in env.Open
	if err != nil && errorToNum(err) > 0 {
		panic(err)
	}

	if err != nil {
		return nil, err
	} else {
		return &DB{env}, nil
	}
}

func (db *DB) Close() {
	err := db.env.Close() // all opened dbis are closed during this process
	if err != nil {       // Possible errors: "Env already closed"
		panic(err)
	}
	db.env = nil
}

func (db *DB) Stat() *Stat {
	stat, err := db.env.Stat()
	if err != nil { // Possible errors: EINVAL
		panic(err)
	}
	return &Stat{*stat}
}

func (db *DB) Info() *Info {
	info, err := db.env.Info()
	if err != nil { // error when env == nil, so panic
		panic(err)
	}
	return &Info{*info}
}

// start a Read-Write txn. The txn will be committed or aborted based on the returning value of {f}
func (db *DB) Update(parent *Txn, f func(*Txn) error) (err error) {
	txnOrig, err := db.env.BeginTxn(parent.txn, 0)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}
	txn := &Txn{txnOrig}

	var panicF interface{} // panic from f

	defer func() {
		if err == nil && panicF == nil {
			txn.commit()
		} else {
			txn.abort()
			if panicF != nil {
				panic(panicF) // re-panic
			}
		}
	}()

	err = func() (err error) {
		defer func() {
			panicF = recover()
		}()
		err = f(txn)
		return
	}()

	return
}

func (db *DB) View(parent *Txn, f func(*Txn)) {
	txnOrig, err := db.env.BeginTxn(parent.txn, mdb.RDONLY)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}
	txn := &Txn{txnOrig}

	var panicF interface{} // panic from f

	defer func() {
		txn.abort() // always abort the txn
		if panicF != nil {
			panic(panicF)
		}
	}()

	func() {
		defer func() {
			panicF = recover()
		}()
		f(txn)
	}()
}
