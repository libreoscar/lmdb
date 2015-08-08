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
// 1) Use iterators only in the txn that they are created; close them before txn ends.
// 2) DO NOT modify the memory slice from tx.Get and iterators.
// 3) Close all read/write txns before DB.Close()

const (
	// On 64-bit there is no penalty for making this huge
	MAP_SIZE_DEFAULT uint64 = 1024 * 1024 * 1024 * 1024 // 1TB
)

type DB mdb.Env

/*
 * A database handle (DBI) denotes the name and parameters of a database, independently of whether
 * such a database exists.
 * The database handle may be discarded by calling #mdb_dbi_close().
 * The old database handle is returned if the database was already open.
 * The handle may only be closed once.
 * The database handle will be private to the current txn until the txn is successfully committed.
 * If the txn is aborted the handle will be closed automatically. After a successful commit the
 * handle will reside in the shared environment, and may be used by other txns. This function must
 * not be called from multiple concurrent txns. A txn that uses this function must finish (either
 * commit or abort) before any other txn may use this function.
 *
 * ref: mdb_dbi_open's doc
 *
 * BTW: In this package, a BucketID(DBI) can be obtained only through Open/Open2, and is never
 * closed until DB.Close(), in which all dbis are closed automatically.
 */
type BucketID mdb.DBI

type Stat mdb.Stat
type Info mdb.Info

//--------------------------------- DB ------------------------------------------------------------

func Version() string {
	return mdb.Version()
}

func Open(path string, buckets []string) (*DB, []BucketID, error) {
	return Open2(path, buckets, MAP_SIZE_DEFAULT)
}

func Open2(path string, buckets []string, maxMapSize uint64) (db *DB, ids []BucketID, err error) {
	env, err := mdb.NewEnv()
	if err != nil {
		return
	}

	err = env.SetMapSize(maxMapSize)
	if err != nil {
		return
	}

	// http://www.openldap.org/lists/openldap-technical/201305/msg00176.html
	err = env.SetMaxDBs((mdb.DBI)(len(buckets)))
	if err != nil {
		return
	}

	MDB_NOTLS := uint(0x200000)
	MDB_NORDAHEAD := uint(0x800000)
	err = env.Open(path, MDB_NOTLS|MDB_NORDAHEAD, 0664)
	if err != nil {
		return
	} else {
		db = (*DB)(env)
	}

	err = db.Update(nil, func(txn *Txn) error {
		for _, name := range buckets {
			id, err := txn.openBucket(name)
			if err != nil {
				return err
			} else {
				ids = append(ids, id)
			}
		}
		return nil
	})

	return
}

func (db *DB) Close() {
	(*mdb.Env)(db).Close() // all opened dbis are closed during this process
}

func (db *DB) Stat() *Stat {
	stat, err := (*mdb.Env)(db).Stat()
	if err != nil { // Possible errors: EINVAL
		panic(err)
	}
	return (*Stat)(stat)
}

func (db *DB) Info() *Info {
	info, err := (*mdb.Env)(db).Info()
	if err != nil { // error when env == nil, so panic
		panic(err)
	}
	return (*Info)(info)
}

// start a Read-Write txn. The txn will be committed or aborted based on the returning value of {f}
func (db *DB) Update(parent *Txn, f func(*Txn) error) (err error) {
	txnOrig, err := (*mdb.Env)(db).BeginTxn((*mdb.Txn)(parent), 0)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}
	txn := (*Txn)(txnOrig)

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
	txnOrig, err := (*mdb.Env)(db).BeginTxn((*mdb.Txn)(parent), mdb.RDONLY)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}
	txn := (*Txn)(txnOrig)

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
