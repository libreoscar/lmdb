package lmdb

import (
	"errors"
	mdb "github.com/szferi/gomdb"
)

// Thread Safety
// 1) NOTLS mode is used exclusively, which allows read txns to freely migrate across
//    threads and for a single thread to maintain multiple read txns. This enables mostly
//    care-free use of read txns, for example when using gevent.
// 2) Most objects can be safely called by a single caller from a single thread, and usually it
//    only makes sense to have a single caller, except in the case of Context.
// 3) Most Context methods are thread-safe, and may be called concurrently, except for
//    Context.Close().
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
// 3) Make sure all read/write txns are finished before Context.Close().

const (
	// There is no penalty for making this huge.
	// If you are on a 32-bit system, use Open2 and specify a smaller map size.
	MAP_SIZE_DEFAULT uint64 = 64 * 1024 * 1024 * 1024 * 1024 // 64TB
)

type Context struct {
	env *mdb.Env
	// In this package, a DBI is obtained only through Open/Open2, and is never closed until
	// Context.Close(), in which all dbis are closed automatically.
	buckets map[string]mdb.DBI
	txn     *mdb.Txn
	// Cached iterators in the current transaction, will be closed when txn finishes.
	itrs []*Iterator
}

type Stat mdb.Stat
type Info mdb.Info

//--------------------------------- DB ------------------------------------------------------------

func Version() string {
	return mdb.Version()
}

func Open(path string, buckets []string) (Context, error) {
	return Open2(path, buckets, MAP_SIZE_DEFAULT)
}

func Open2(path string, buckets []string, maxMapSize uint64) (ctx Context, err error) {
	// TODO (Potential bug):
	// From mdb_env_open's doc,
	//  "If this function fails, #mdb_env_close() must be called to discard the #MDB_env handle."
	// But mdb.NewEnv doesnot call mdb_env_close() when it fails, AND it just return nil as env.
	// Patch gomdb if this turns out to be a big issue.
	env, err := mdb.NewEnv()
	defer func() {
		if err != nil && env != nil {
			env.Close()
			ctx.env = nil
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
	ctx = Context{env, nil, nil, nil}

	err = ctx.Transactional(true, func(ctx *Context) error {
		for _, name := range buckets {
			if name == "" {
				return errors.New("Bucket name is empty")
			}
			dbi, err := ctx.txn.DBIOpen(&name, mdb.CREATE)
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
		ctx.buckets = bucketCache
	}

	return
}

func (ctx *Context) CloseDB() {
	if ctx.txn != nil {
		panic("Closing database inside a transaction")
	}
	if ctx.env != nil {
		ctx.env.Close() // all opened dbis are closed during this process
	}
}

func (ctx *Context) DBStat() *Stat {
	stat, err := ctx.env.Stat()
	if err != nil { // Possible errors: EINVAL
		panic(err)
	}
	return (*Stat)(stat)
}

func (ctx *Context) Info() *Info {
	info, err := ctx.env.Info()
	if err != nil { // error when env == nil, so panic
		panic(err)
	}
	return (*Info)(info)
}

func (ctx *Context) Transactional(write bool, f func(ctx *Context) error) (err error) {
	var flag uint = 0
	if !write {
		flag = mdb.RDONLY
	}
	txn, err := ctx.env.BeginTxn(ctx.txn, flag)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}
	var panicF interface{} // panic from f
	newCtx := Context{ctx.env, ctx.buckets, txn, nil}

	defer func() {
		for _, itr := range newCtx.itrs {
			itr.Close() // no panic
		}
		newCtx.itrs = nil

		if err == nil && panicF == nil && write {
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

	err = func() (err error) {
		defer func() {
			panicF = recover()
		}()
		err = f(&newCtx)
		return
	}()

	return
}

// panic if {bucket} does not exist, internal use
func (ctx *Context) getBucketId(bucket string) mdb.DBI {
	id, b := ctx.buckets[bucket]
	if !b {
		panic("bucket does not exist")
	} else {
		return id
	}
}

func (ctx *Context) ClearBucket(bucket string) {
	err := ctx.txn.Drop(ctx.getBucketId(bucket), 0)
	if err != nil { // Possible errors: EINVAL, EACCES, MDB_BAD_DBI
		panic(err)
	}
}

func (ctx *Context) BucketStat(bucket string) *Stat {
	stat, err := ctx.txn.Stat(ctx.getBucketId(bucket))
	if err != nil { // Possible errors: EINVAL, MDB_BAD_TXN
		panic(err)
	}
	return (*Stat)(stat)
}

// Return {nil, false} if {key} does not exist, {val, true} if {key} exist
func (ctx *Context) Get(bucket string, key []byte) ([]byte, bool) {
	v, err := ctx.txn.GetVal(ctx.getBucketId(bucket), key)
	if err != nil {
		if err == mdb.NotFound {
			return nil, false
		} else { // Possible errors: EINVAL, MDB_BAD_TXN, MDB_BAD_VALSIZE, etc
			panic(err)
		}
	}
	return v.Bytes(), true
}

// 1) Return {nil, false} if {key} does not exist, {val, true} if {key} exist
func (ctx *Context) GetNoCopy(bucket string, key []byte) ([]byte, bool) {
	v, err := ctx.txn.GetVal(ctx.getBucketId(bucket), key)
	if err != nil {
		if err == mdb.NotFound {
			return nil, false
		} else { // Possible errors: EINVAL, MDB_BAD_TXN, MDB_BAD_VALSIZE, etc
			panic(err)
		}
	}
	return v.BytesNoCopy(), true
}

func (ctx *Context) Put(bucket string, key, val []byte) {
	err := ctx.txn.Put(ctx.getBucketId(bucket), key, val, 0)
	if err != nil { // Possible errors: MDB_MAP_FULL, MDB_TXN_FULL, EACCES, EINVAL
		panic(err)
	}
}

func (ctx *Context) Delete(bucket string, key []byte) {
	err := ctx.txn.Del(ctx.getBucketId(bucket), key, nil)
	if err != nil && err != mdb.NotFound { // Possible errors: EINVAL, EACCES, MDB_BAD_TXN
		panic(err)
	}
}

// Return an iterator pointing to the first item in the bucket.
// If the bucket is empty, nil is returned.
func (ctx *Context) Iterate(bucket string) *Iterator {
	cur, err := ctx.txn.CursorOpen(ctx.getBucketId(bucket))
	if err != nil {
		panic(err)
	}

	itr := (*Iterator)(cur)

	if itr.SeekFirst() {
		ctx.itrs = append(ctx.itrs, itr)
		return itr
	} else {
		itr.Close()
		return nil
	}
}
