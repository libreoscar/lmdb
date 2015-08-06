package lmdb

// Thread Safety
//
// 1) NOTLS mode is used exclusively, which allows read transactions to freely migrate across
//    threads and for a single thread to maintain multiple read transactions. This enables mostly
//    care-free use of read transactions, for example when using gevent.
//
// 2) Most objects can be safely called by a single caller from a single thread, and usually it
//    only makes sense to to have a single caller, except in the case of DB.
//
// 3) Most DB methods are thread-safe, and may be called concurrently, except for DB.close().
//
// 4) A write Transaction (DB.Update()) may only be used from the thread it was created on.
//
// 5) A read-only Transaction (DB.View()) can move across threads, but it cannot be used
//    concurrently from multiple threads.
//
// 6) Cursor is not thread-safe, but it does not make sense to use it on any thread except the
//    thread that currently owns its associated Transaction.
//
//-------------------------------------------------------------------------------------------------
//
// Best practice:
// 1) Create/Drop tables in the beginning of the program, before any other txns; do not drop or
//    open tables when the program runs in parallel afterwards
// 2) Create cursors only when inside tx; close cursors before tx ends.
// 3) Close all read/write txns before DB.Close()

import (
	"github.com/szferi/gomdb"
	"syscall"
)

const (
	// Named databases + main and free DB are 30 + 2 == 32
	MAX_TABLES_DEFAULT uint = 30

	// On 64-bit there is no penalty for making this huge
	MAP_SIZE_DEFAULT uint64 = 1024 * 1024 * 1024 * 1024 // 1TB

	ERR_TABLE_NAME_EMPTY = "Table name can not be empty"
	ERR_WRITE_TXN_ONLY   = "The operation is only allowed in a write transaction"
)

type DB struct {
	env *mdb.Env
}

/*
 * A database handle (DBI) denotes the name and parameters of a database,
 * independently of whether such a database exists.
 * The database handle may be discarded by calling #mdb_dbi_close().
 * The old database handle is returned if the database was already open.
 * The handle may only be closed once.
 * The database handle will be private to the current transaction until
 * the transaction is successfully committed. If the transaction is
 * aborted the handle will be closed automatically.
 * After a successful commit the
 * handle will reside in the shared environment, and may be used
 * by other transactions. This function must not be called from
 * multiple concurrent transactions. A transaction that uses this function
 * must finish (either commit or abort) before any other transaction may
 * use this function.
 *
 * ref: mdb_dbi_open's doc
 *
 * BTW: In golmdb2, a TableID(DBI) is never closed (as we didn't provide apis for calling
 * mdb_dbi_close) until DB.Close() is called
 */
type TableID mdb.DBI

// managed by DB.Update & DB.View
type Txn struct {
	txn   *mdb.Txn
	write bool // read-only or read-write
}

// In a write txn, it will be closed automatically when the txn commits/aborts
// In a read-only txn, it must be closed explicitly
type Cursor struct {
	cursor *mdb.Cursor
}

// Call Bytes() or BytesNoCopy() to get a []byte type result
// TODO: is it possible to make it a slice?
type MemSlice struct {
	mdb.Val
}

type Stat struct {
	mdb.Stat
}

type Info struct {
	mdb.Info
}

//--------------------------------- Helpers -------------------------------------------------------

// TODO: test with nil err
func errorToNum(err error) int {
	if v, ok := err.(mdb.Errno); ok {
		return int(v)
	}
	if v, ok := err.(syscall.Errno); ok {
		return int(v)
	}
	panic("Invalid input")
}

func bool2i(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

//-------------------------------------------------------------------------------------------------

func Version() string {
	return mdb.Version()
}

//--------------------------------- DB ------------------------------------------------------------

func Open(path string) (*DB, error) {
	return Open2(path, MAX_TABLES_DEFAULT, MAP_SIZE_DEFAULT)
}

func Open2(path string, maxTables uint, maxMapSize uint64) (*DB, error) {
	env, err := mdb.NewEnv()
	if err != nil { // Possible errors: ENOMEM (out of memory)
		panic(err)
	}

	err = env.SetMapSize(maxMapSize)
	if err != nil { // Possible errors: EINVAL, other system criticle errors
		panic(err)
	}

	// http://www.openldap.org/lists/openldap-technical/201305/msg00176.html
	err = env.SetMaxDBs((mdb.DBI)(maxTables))
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
	if parent != nil && !parent.write {
		panic("Write txn in a read txn")
	}

	txnOrig, err := db.env.BeginTxn(parent.txn, 0)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}
	txn := &Txn{txnOrig, true}

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
	txn := &Txn{txnOrig, false}

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

//--------------------------------- TX ------------------------------------------------------------

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

// Create a table if it does not exist
// There's no TableID.Close() by purpose; all tables are closed automatically in DB.Close()
// DO NOT CALL THIS CONCURRENTLY!!!
func (txn *Txn) OpenTable(name string) TableID {
	if name == "" {
		panic(ERR_TABLE_NAME_EMPTY)
	}
	if !txn.write {
		panic(ERR_WRITE_TXN_ONLY)
	}

	dbi, err := txn.txn.DBIOpen(&name, mdb.CREATE)
	if err != nil { // Possible errors: MDB_DBS_FULL
		// because MDB_DBS_FULL is controllable by the programmer, it deserves a panic
		panic(err)
	}
	return (TableID)(dbi)
}

// 1) Silent if {id} does not exist (TODO: test the behaviour when id does not exist)
// 2) if {del} is true, the table will be removed (warning: the table id may be unusable in the
//    following txns); if {del} is false, table is only cleared, {id} is still valid.
func (txn *Txn) DropTable(id TableID, del bool) {
	err := txn.txn.Drop((mdb.DBI)(id), bool2i(del)) // TODO: ignore the errors?
	if err != nil {                                 // Possible errors: EINVAL, EACCES, MDB_BAD_DBI
		panic(err)
	}
}

// Panic if the specified table does not exist
func (txn *Txn) Stat(id TableID) *Stat {
	stat, err := txn.txn.Stat((mdb.DBI)(id))
	if err != nil { // Possible errors: EINVAL, MDB_BAD_TXN
		panic(err)
	}
	return &Stat{*stat}
}

// 1) Panic if {id} is invalid
// 2) Return {zero, false} if {key} does not exist, {val, true} if {key} exist
func (txn *Txn) Get(id TableID, key []byte) (MemSlice, bool) {
	v, err := txn.txn.GetVal((mdb.DBI)(id), key)
	if err != nil {
		if err == mdb.NotFound {
			return MemSlice{}, false
		} else { // Possible errors: EINVAL, MDB_BAD_TXN, MDB_BAD_VALSIZE, etc
			panic(err)
		}
	}
	return MemSlice{v}, true
}

// 1) Panic if {id} is invalid
func (txn *Txn) Put(id TableID, key, val []byte) {
	err := txn.txn.Put((mdb.DBI)(id), key, val, 0)
	if err != nil { // Possible errors: MDB_MAP_FULL, MDB_TXN_FULL, EACCES, EINVAL
		panic(err) // TODO: test the condition of MDB_TXN_FULL (seemes 2^17, make sure tx is safe)
	}
}

// 1) Silent if {key} does not exist
// TODO: test with non-existence keys (error is MDB_NOTFOUND?)
func (txn *Txn) Delete(id TableID, key []byte) {
	err := txn.txn.Del((mdb.DBI)(id), key, nil)
	if err != nil { // Possible errors: EINVAL, EACCES, MDB_BAD_TXN
		panic(err)
	}
}

// TODO: test behaviour
// 1) when db is empty (in this case, there should not be a panic)
// 2) test cursor location
func (txn *Txn) SeekFirst(id TableID) (*Cursor, error) {
	cursor, err := txn.txn.CursorOpen((mdb.DBI)(id))
	if err != nil {
		panic(err)
	} else {
		return &Cursor{cursor}, nil // TODO
	}
}

//--------------------------------- Cursor --------------------------------------------------------

func (cursor *Cursor) Close() {
	if cursor.cursor == nil {
		return
	}
	cursor.cursor.Close() // Possible errors: Cursor already closed, ignored
	cursor.cursor = nil
}

func (cursor *Cursor) SeekFirst() (MemSlice, MemSlice, bool) {
	key, val, err := cursor.cursor.GetVal(nil, nil, mdb.FIRST)
	// TODO: test & process err
	return MemSlice{key}, MemSlice{val}, (err == nil)
}

func (cursor *Cursor) SeekLast() (MemSlice, MemSlice, bool) {
	key, val, err := cursor.cursor.GetVal(nil, nil, mdb.LAST)
	// TODO: test & process err
	return MemSlice{key}, MemSlice{val}, (err == nil)
}

func (cursor *Cursor) Previous() (MemSlice, MemSlice, bool) {
	key, val, err := cursor.cursor.GetVal(nil, nil, mdb.PREV)
	// TODO: test & process err
	return MemSlice{key}, MemSlice{val}, (err == nil)
}

func (cursor *Cursor) Next() (MemSlice, MemSlice, bool) {
	key, val, err := cursor.cursor.GetVal(nil, nil, mdb.NEXT)
	// TODO: test & process err
	return MemSlice{key}, MemSlice{val}, (err == nil)
}

func (cursor *Cursor) SeekTo(k []byte) (MemSlice, MemSlice, bool) { // SET TODO: test behaviour
	key, val, err := cursor.cursor.GetVal(k, nil, mdb.SET_RANGE)
	// TODO: test & process err
	return MemSlice{key}, MemSlice{val}, (err == nil)
}

func (cursor *Cursor) Get() (MemSlice, MemSlice, bool) { // GET_CURRENT
	key, val, err := cursor.cursor.GetVal(nil, nil, mdb.GET_CURRENT)
	// TODO: test & process err
	return MemSlice{key}, MemSlice{val}, (err == nil)
}

// TODO: test behaviour
func (cursor *Cursor) Put(key, val []byte) {
	err := cursor.cursor.Put(key, val, 0)
	// TODO: test & process err
	if err != nil {
		panic(err)
	}
}

func (cursor *Cursor) Delete() {
	err := cursor.cursor.Del(0)
	if err != nil {
		panic(err)
	}
}
