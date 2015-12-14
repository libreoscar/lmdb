package lmdb

import (
	"errors"
	"fmt"
	mdb "github.com/libreoscar/gomdb"
)

type ReadTxner interface {
	BucketStat(bucket string) *Stat
	Get(bucket string, key []byte) ([]byte, bool)
	GetNoCopy(bucket string, key []byte) ([]byte, bool)
	Iterate(bucket string) *Iterator
}

type ReadTxn struct {
	// In this package, a DBI is obtained only through Open/Open2, and is never closed until
	// Context.Close(), in which all dbis are closed automatically.
	buckets map[string]mdb.DBI
	txn     *mdb.Txn
	// Cached iterators in the current transaction, will be closed when txn finishes.
	itrs []*Iterator
}

type ReadWriteTxn struct {
	env *mdb.Env
	*ReadTxn
	dirtyKeys map[string]bool // the key is serilized CellKey
}

//--------------------------------- ReadTxn -------------------------------------------------------

// panic if {bucket} does not exist, internal use
func (txn *ReadTxn) getBucketId(bucket string) mdb.DBI {
	id, b := txn.buckets[bucket]
	if !b {
		panic(fmt.Errorf("bucket does not exist: %s", bucket))
	} else {
		return id
	}
}

func (txn *ReadTxn) BucketStat(bucket string) *Stat {
	stat, err := txn.txn.Stat(txn.getBucketId(bucket))
	if err != nil { // Possible errors: EINVAL, MDB_BAD_TXN
		panic(err)
	}
	return (*Stat)(stat)
}

// Return {nil, false} if {key} does not exist, {val, true} if {key} exist
func (txn *ReadTxn) Get(bucket string, key []byte) ([]byte, bool) {
	v, err := txn.txn.GetVal(txn.getBucketId(bucket), key)
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
func (txn *ReadTxn) GetNoCopy(bucket string, key []byte) ([]byte, bool) {
	v, err := txn.txn.GetVal(txn.getBucketId(bucket), key)
	if err != nil {
		if err == mdb.NotFound {
			return nil, false
		} else { // Possible errors: EINVAL, MDB_BAD_TXN, MDB_BAD_VALSIZE, etc
			panic(err)
		}
	}
	return v.BytesNoCopy(), true
}

// Return an iterator pointing to the first item in the bucket.
// If the bucket is empty, nil is returned.
func (txn *ReadTxn) Iterate(bucket string) *Iterator {
	cur, err := txn.txn.CursorOpen(txn.getBucketId(bucket))
	if err != nil {
		panic(err)
	}

	itr := (*Iterator)(cur)

	if itr.SeekFirst() {
		txn.itrs = append(txn.itrs, itr)
		return itr
	} else {
		itr.Close()
		return nil
	}
}

//--------------------------------- ReadWriteTxn --------------------------------------------------

func (parent *ReadWriteTxn) TransactionalRW(f func(*ReadWriteTxn) error) (err error) {
	txn, err := parent.env.BeginTxn(parent.txn, 0)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}

	var panicF interface{} // panic from f
	var subDirtyKeys map[string]bool
	if parent.dirtyKeys != nil {
		subDirtyKeys = make(map[string]bool)
	}
	rwCtx := ReadWriteTxn{parent.env, &ReadTxn{parent.buckets, txn, nil}, subDirtyKeys}

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

			if (parent.dirtyKeys == nil) != (rwCtx.dirtyKeys == nil) {
				panic(fmt.Errorf("unexpected error"))
			}
			for dirtyKey := range rwCtx.dirtyKeys {
				parent.dirtyKeys[dirtyKey] = true
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

func (txn *ReadWriteTxn) ApplyPatch(patch TxnPatch) error {
	for _, cell := range patch {
		if cell.exists {
			txn.Put(cell.bucket, cell.key, cell.value)
		} else {
			txn.Delete(cell.bucket, cell.key)
		}
	}
	return nil
}

func (txn *ReadWriteTxn) ClearBucket(bucket string) {
	err := txn.txn.Drop(txn.getBucketId(bucket), 0)
	if err != nil { // Possible errors: EINVAL, EACCES, MDB_BAD_DBI
		panic(err)
	}

	if txn.dirtyKeys != nil {
		// currently we do not support this operation when making a TxnPatch
		panic(errors.New("Encountered ClearBucket operation when making a TxnPatch"))
	}
}

func (txn *ReadWriteTxn) Put(bucket string, key, val []byte) {
	err := txn.txn.Put(txn.getBucketId(bucket), key, val, 0)
	if err != nil { // Possible errors: MDB_MAP_FULL, MDB_TXN_FULL, EACCES, EINVAL
		panic(err)
	}

	if txn.dirtyKeys != nil {
		txn.dirtyKeys[CellKey{bucket, key}.Serialize()] = true
	}
}

func (txn *ReadWriteTxn) Delete(bucket string, key []byte) {
	err := txn.txn.Del(txn.getBucketId(bucket), key, nil)
	if err != nil && err != mdb.NotFound { // Possible errors: EINVAL, EACCES, MDB_BAD_TXN
		panic(err)
	}

	if txn.dirtyKeys != nil {
		txn.dirtyKeys[CellKey{bucket, key}.Serialize()] = true
	}
}
