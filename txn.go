package lmdb

import (
	"errors"
	"github.com/szferi/gomdb"
)

// managed by DB.Update(Write Txn) & DB.View(Read Txn)
type Txn mdb.Txn

// Used internally.
func (txn *Txn) commit() {
	e := (*mdb.Txn)(txn).Commit()
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
	(*mdb.Txn)(txn).Abort()
}

// Used internally
// Create a bucket if it does not exist
// There's no BucketID.Close() by purpose; all buckets are closed automatically in DB.Close()
func (txn *Txn) openBucket(name string) (id BucketID, err error) {
	if name == "" {
		err = errors.New("Bucket name is empty")
		return
	}
	if dbi, err := (*mdb.Txn)(txn).DBIOpen(&name, mdb.CREATE); err == nil {
		id = (BucketID)(dbi)
	}
	return
}

// 1) Panic if {id} does not exist (TODO: test the behaviour when id does not exist)
func (txn *Txn) ClearBucket(id BucketID) {
	err := (*mdb.Txn)(txn).Drop((mdb.DBI)(id), 0)
	if err != nil { // Possible errors: EINVAL, EACCES, MDB_BAD_DBI
		panic(err)
	}
}

// Panic if the specified bucket does not exist
func (txn *Txn) Stat(id BucketID) *Stat {
	stat, err := (*mdb.Txn)(txn).Stat((mdb.DBI)(id))
	if err != nil { // Possible errors: EINVAL, MDB_BAD_TXN
		panic(err)
	}
	return (*Stat)(stat)
}

// 1) Panic if {id} is invalid
// 2) Return {nil, false} if {key} does not exist, {val, true} if {key} exist
func (txn *Txn) Get(id BucketID, key []byte) ([]byte, bool) {
	v, err := (*mdb.Txn)(txn).GetVal((mdb.DBI)(id), key)
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
	err := (*mdb.Txn)(txn).Put((mdb.DBI)(id), key, val, 0)
	if err != nil { // Possible errors: MDB_MAP_FULL, MDB_TXN_FULL, EACCES, EINVAL
		panic(err)
	}
}

// 1) Silent if {key} does not exist
func (txn *Txn) Delete(id BucketID, key []byte) {
	err := (*mdb.Txn)(txn).Del((mdb.DBI)(id), key, nil)
	if err != nil && err != mdb.NotFound { // Possible errors: EINVAL, EACCES, MDB_BAD_TXN
		panic(err)
	}
}

// TODO: test behaviour
// 1) when bucket is empty (in this case, there should not be a panic)
// 2) test cur location
func (txn *Txn) BucketBegin(id BucketID) *Iterator {
	cur, err := (*mdb.Txn)(txn).CursorOpen((mdb.DBI)(id))
	if err != nil {
		panic(err)
	} else {
		return (*Iterator)(cur)
	}
}
