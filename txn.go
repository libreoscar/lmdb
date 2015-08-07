package lmdb

import (
	"github.com/szferi/gomdb"
)

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
 * BTW: In this package, a BucketID(DBI) is never closed (as there's no mdb_dbi_close like
 * api is provided) until DB.Close(), in which all dbis are closed automatically.
 */
type BucketID mdb.DBI

// managed by DB.Update(Write Txn) & DB.View(Read Txn)
type Txn struct {
	txn *mdb.Txn
}

// Used internally.
func (txn *Txn) commit() {
	e := txn.txn.Commit()
	txn.txn = nil
	// Possible errors:
	// EINVAL - invalid parameters
	// ENOSPEC - no more disk spaces
	// EIO - low-level I/O error
	// ENOMEM - out of memory
	if e != nil {
		panic(e)
	}
}

// Used internally.
func (txn *Txn) abort() {
	txn.txn.Abort()
	txn.txn = nil
}

// Create a bucket if it does not exist
// There's no BucketID.Close() by purpose; all buckets are closed automatically in DB.Close()
// DO NOT CALL THIS CONCURRENTLY!!!
func (txn *Txn) OpenBucket(name string) BucketID {
	if name == "" {
		panic("Bucket name can not be empty")
	}

	dbi, err := txn.txn.DBIOpen(&name, mdb.CREATE)
	if err != nil { // Possible errors: MDB_DBS_FULL
		// because MDB_DBS_FULL is controllable by the programmer, it deserves a panic
		panic(err)
	}
	return (BucketID)(dbi)
}

// 1) Panic if {id} does not exist (TODO: test the behaviour when id does not exist)
func (txn *Txn) ClearBucket(id BucketID) {
	err := txn.txn.Drop((mdb.DBI)(id), 0) // TODO: ignore the errors?
	if err != nil {                       // Possible errors: EINVAL, EACCES, MDB_BAD_DBI
		panic(err)
	}
}

// 1) Silent if {id} does not exist (TODO: test the behaviour when id does not exist)
// 2) if {del} is true, the bucket will be removed (warning: the bucket id may be unusable in the
//    following txns); if {del} is false, bucket is only cleared, {id} is still valid.
func (txn *Txn) DropBucket(id BucketID) {
	err := txn.txn.Drop((mdb.DBI)(id), 1) // TODO: ignore the errors?
	if err != nil {                       // Possible errors: EINVAL, EACCES, MDB_BAD_DBI
		panic(err)
	}
}

// Panic if the specified bucket does not exist
func (txn *Txn) Stat(id BucketID) *Stat {
	stat, err := txn.txn.Stat((mdb.DBI)(id))
	if err != nil { // Possible errors: EINVAL, MDB_BAD_TXN
		panic(err)
	}
	return &Stat{*stat}
}

// 1) Panic if {id} is invalid
// 2) Return {nil, false} if {key} does not exist, {val, true} if {key} exist
func (txn *Txn) Get(id BucketID, key []byte) ([]byte, bool) {
	v, err := txn.txn.GetVal((mdb.DBI)(id), key)
	if err != nil {
		if err == mdb.NotFound {
			return nil, false
		} else { // Possible errors: EINVAL, MDB_BAD_TXN, MDB_BAD_VALSIZE, etc
			panic(err)
		}
	}
	return v.BytesNoCopy(), true
}

// 1) Panic if {id} is invalid
func (txn *Txn) Put(id BucketID, key, val []byte) {
	err := txn.txn.Put((mdb.DBI)(id), key, val, 0)
	if err != nil { // Possible errors: MDB_MAP_FULL, MDB_TXN_FULL, EACCES, EINVAL
		panic(err) // TODO: test the condition of MDB_TXN_FULL (seemes 2^17, make sure tx is safe)
	}
}

// 1) Silent if {key} does not exist
// TODO: test with non-existence keys (error is MDB_NOTFOUND?)
func (txn *Txn) Delete(id BucketID, key []byte) {
	err := txn.txn.Del((mdb.DBI)(id), key, nil)
	if err != nil { // Possible errors: EINVAL, EACCES, MDB_BAD_TXN
		panic(err)
	}
}

// TODO: test behaviour
// 1) when db is empty (in this case, there should not be a panic)
// 2) test cur location
func (txn *Txn) Begin(id BucketID) (*Iterator, error) {
	cur, err := txn.txn.CursorOpen((mdb.DBI)(id))
	if err != nil {
		panic(err)
	} else {
		return &Iterator{cur}, nil // TODO
	}
}
