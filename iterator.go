package lmdb

import (
	"github.com/szferi/gomdb"
)

// In a write txn, iterator will be closed automatically when the txn commits/aborts, but in a
// read txn, they must be closed explicitly. Thus the best practice is to close them all explicitly
// before txn ends.
//
// Attention:
// The bytes returned from the following functions are read-only, DO NOT write to them.
type Iterator mdb.Cursor

func (itr *Iterator) Close() {
	err := (*mdb.Cursor)(itr).Close() // Possible errors: Iterator already closed
	if err != nil {
		panic(err)
	}
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

func (itr *Iterator) Prev() bool {
	_, _, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.PREV)
	if err != nil && err != mdb.NotFound {
		panic(err)
	}
	return err == nil
}

// If {itr}'s current position is the last element, Next() returns (nil, nil, false), and {itr}
// stays its current position
func (itr *Iterator) Next() bool {
	_, _, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.NEXT)
	if err != nil && err != mdb.NotFound {
		panic(err)
	}
	return err == nil
}

func (itr *Iterator) Seek(k []byte) bool {
	_, _, err := (*mdb.Cursor)(itr).GetVal(k, nil, mdb.SET_RANGE)
	if err != nil && err != mdb.NotFound {
		panic(err)
	}
	return err == nil
}

func (itr *Iterator) Get() ([]byte, []byte) { // GET_CURRENT
	key, val, err := (*mdb.Cursor)(itr).GetVal(nil, nil, mdb.GET_CURRENT)
	if err != nil {
		panic(err)
	}
	return key.BytesNoCopy(), val.BytesNoCopy()
}
