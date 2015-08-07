package lmdb

import (
	"github.com/szferi/gomdb"
)

// In a write txn, iterator will be closed automatically when the txn commits/aborts, but in a
// read txn, they must be closed explicitly. Thus the best practice is to close them all explicitly
// before txn ends.
type Iterator mdb.Cursor

// Attention:
// The bytes returned from the following functions are read-only, DO NOT write to them.

func (itr *Iterator) Close() {
	(*mdb.Cursor)(itr).Close() // Possible errors: Iterator already closed, ignored
}

func (itr *Iterator) SeekFirst() ([]byte, []byte, bool) {
	key, val, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.FIRST)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

func (itr *Iterator) SeekLast() ([]byte, []byte, bool) {
	key, val, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.LAST)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

func (itr *Iterator) Previous() ([]byte, []byte, bool) {
	key, val, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.PREV)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

func (itr *Iterator) Next() ([]byte, []byte, bool) {
	key, val, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.NEXT)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

func (itr *Iterator) SeekTo(k []byte) ([]byte, []byte, bool) { // SET TODO: test behaviour
	key, val, err := (*mdb.Cursor)(itr).GetVal(k, nil, mdb.SET_RANGE)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

func (itr *Iterator) Get() ([]byte, []byte, bool) { // GET_CURRENT
	key, val, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.GET_CURRENT)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

// TODO: test behaviour
func (itr *Iterator) Put(key, val []byte) {
	err := (*mdb.Cursor)(itr).Put(key, val, 0)
	// TODO: test & process err
	if err != nil {
		panic(err)
	}
}

func (itr *Iterator) Delete() {
	err := (*mdb.Cursor)(itr).Del(0)
	if err != nil {
		panic(err)
	}
}
