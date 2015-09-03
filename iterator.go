package lmdb

import (
	"bytes"
	"github.com/szferi/gomdb"
)

// In a write txn, iterator will be closed automatically when the txn commits/aborts, but in a
// read txn, they must be closed explicitly. Thus the best practice is to close them all explicitly
// before txn ends.
//
// Attention:
// The bytes returned from GetNoCopy() are memory-mapped database contents, DO NOT modify them.
type Iterator mdb.Cursor

func (itr *Iterator) Close() {
	(*mdb.Cursor)(itr).Close() // Possible errors: Iterator already closed (ignored)
}

func (itr *Iterator) SeekFirst() bool {
	_, _, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.FIRST)
	if err != nil && err != mdb.NotFound {
		panic(err)
	}
	return err == nil
}

func (itr *Iterator) SeekLast() bool {
	_, _, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.LAST)
	if err != nil && err != mdb.NotFound {
		panic(err)
	}
	return err == nil
}

// If current position is the first element, Prev() returns false, and stays its current position.
func (itr *Iterator) Prev() bool {
	_, _, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.PREV)
	if err != nil && err != mdb.NotFound {
		panic(err)
	}
	return err == nil
}

// If current position is the last element, Next() returns false, and stays its current position.
func (itr *Iterator) Next() bool {
	_, _, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.NEXT)
	if err != nil && err != mdb.NotFound {
		panic(err)
	}
	return err == nil
}

// Position at the key that matches {k} exactly
func (itr *Iterator) SeekExact(k []byte) bool {
	key, _, err := (*mdb.Cursor)(itr).GetVal(k, nil, mdb.SET_RANGE)
	if err != nil && err != mdb.NotFound {
		panic(err)
	}
	return err == nil && bytes.Equal(key.BytesNoCopy(), k)
}

// Position at first key greater than or equal to specified key.
func (itr *Iterator) SeekGE(k []byte) bool {
	_, _, err := (*mdb.Cursor)(itr).GetVal(k, nil, mdb.SET_RANGE)
	if err != nil && err != mdb.NotFound {
		panic(err)
	}
	return err == nil
}

// Position at first key that has the specified prefix
func (itr *Iterator) SeekByPrefix(prefix []byte) bool {
	key, _, err := (*mdb.Cursor)(itr).GetVal(prefix, nil, mdb.SET_RANGE)
	if err != nil && err != mdb.NotFound {
		panic(err)
	}
	return err == nil && bytes.HasPrefix(key.BytesNoCopy(), prefix)
}

// Returns (key, value) pair.
func (itr *Iterator) Get() ([]byte, []byte) {
	key, val, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.GET_CURRENT)
	if err != nil {
		panic(err)
	}
	return key.Bytes(), val.Bytes()
}

// Returns (key, value) pair. DO NOT modify them in-place, make a copy instead.
func (itr *Iterator) GetNoCopy() ([]byte, []byte) {
	key, val, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.GET_CURRENT)
	if err != nil {
		panic(err)
	}
	return key.BytesNoCopy(), val.BytesNoCopy()
}
