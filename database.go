package lmdb

import (
	"errors"
	mdb "github.com/szferi/gomdb"
)

// Thread Safety
// 1) NOTLS mode is used exclusively, which allows read txns to freely migrate across
//    threads and for a single thread to maintain multiple read txns. This enables mostly
//    care-free use of read txns.
// 2) Most objects can be safely called by a single caller from a single thread, and usually it
//    only makes sense to have a single caller, except in the case of Database.
// 3) Most Database methods are thread-safe, and may be called concurrently, except for
//    Database.Close().
// 4) A write txn may only be used from the thread it was created on.
// 5) A read-only txn can move across threads, but it cannot be used concurrently from multiple
//    threads.
// 6) Iterator is not thread-safe, but it does not make sense to use it on any thread except the
//    thread that currently owns its associated txn.
//
//-------------------------------------------------------------------------------------------------
//
// Best practice:
// 1) Use iterators only in the txn that they are created
// 2) DO NOT modify the memory slice from GetNoCopy
// 3) Make sure all read/write txns are finished before Database.Close().

const (
	// There is no penalty for making this huge.
	// If you are on a 32-bit system, use Open2 and specify a smaller map size.
	MAP_SIZE_DEFAULT uint64 = 64 * 1024 * 1024 * 1024 * 1024 // 64TB
)

type TransactionalRWer interface {
	TransactionalRW(func(*ReadWriteTxn) error) error
}

type Database struct {
	env *mdb.Env
	// In this package, a DBI is obtained only through Open/Open2, and is never closed until
	// Context.Close(), in which all dbis are closed automatically.
	buckets map[string]mdb.DBI
}

type Stat mdb.Stat
type Info mdb.Info

//--------------------------------- Database ------------------------------------------------------

func Version() string {
	return mdb.Version()
}

func Open(path string, buckets []string) (*Database, error) {
	return Open2(path, buckets, MAP_SIZE_DEFAULT)
}

func Open2(path string, buckets []string, maxMapSize uint64) (db *Database, err error) {
	// TODO (Potential bug):
	// From mdb_env_open's doc,
	//  "If this function fails, #mdb_env_close() must be called to discard the #MDB_env handle."
	// But mdb.NewEnv doesnot call mdb_env_close() when it fails, AND it just return nil as env.
	// Patch gomdb if this turns out to be a big issue.
	env, err := mdb.NewEnv()
	defer func() {
		if err != nil && env != nil {
			env.Close()
			db.env = nil
		}
	}()
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
	}

	bucketCache := make(map[string]mdb.DBI)
	db = &Database{env, nil}

	err = db.TransactionalRW(func(txn *ReadWriteTxn) error {
		for _, name := range buckets {
			if name == "" {
				return errors.New("Bucket name is empty")
			}
			dbi, err := txn.txn.DBIOpen(&name, mdb.CREATE)
			if err != nil {
				return err
			} else {
				bucketCache[name] = dbi
			}
		}
		return nil
	})

	if err != nil {
		return
	} else {
		db.buckets = bucketCache
	}

	return
}

func (db *Database) Close() {
	if db.env != nil {
		db.env.Close() // all opened dbis are closed during this process
	}
}

func (db *Database) Stat() *Stat {
	stat, err := db.env.Stat()
	if err != nil { // Possible errors: EINVAL
		panic(err)
	}
	return (*Stat)(stat)
}

func (db *Database) Info() *Info {
	info, err := db.env.Info()
	if err != nil { // error when env == nil, so panic
		panic(err)
	}
	return (*Info)(info)
}

func (db *Database) TransactionalR(f func(*ReadTxn)) {
	txn, err := db.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}

	var panicF interface{} // panic from f
	rdTxn := ReadTxn{db.buckets, txn, nil}

	defer func() {
		for _, itr := range rdTxn.itrs {
			itr.Close() // no panic
		}
		rdTxn.itrs = nil

		txn.Abort()
		if panicF != nil {
			panic(panicF) // re-panic
		}
	}()

	func() {
		defer func() {
			panicF = recover()
		}()
		f(&rdTxn)
	}()
}

func (db *Database) TransactionalRW(f func(*ReadWriteTxn) error) (err error) {
	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}

	var panicF interface{} // panic from f
	rwCtx := ReadWriteTxn{db.env, &ReadTxn{db.buckets, txn, nil}}

	defer func() {
		for _, itr := range rwCtx.itrs {
			itr.Close() // no panic
		}
		rwCtx.itrs = nil

		if err == nil && panicF == nil {
			e := txn.Commit()
			if e != nil { // Possible errors: EINVAL, ENOSPEC, EIO, ENOMEM
				panic(e)
			}
		} else {
			txn.Abort()
			if panicF != nil {
				panic(panicF) // re-panic
			}
		}
	}()

	func() {
		defer func() {
			panicF = recover()
		}()
		err = f(&rwCtx)
	}()

	return
}
