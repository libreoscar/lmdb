package lmdb

import (
	"github.com/szferi/gomdb"
)

// In a write txn, iterator will be closed automatically when the txn commits/aborts, but in a
// read txn, they must be closed explicitly. Thus the best practice is to close them all explicitly
// before txn ends.
type Iterator struct {
	cur *mdb.Cursor
}

// Attention:
// The bytes returned from the following functions are read-only, DO NOT write to them.

func (cur *Iterator) Close() {
	if cur.cur == nil {
		return
	}
	cur.cur.Close() // Possible errors: Iterator already closed, ignored
	cur.cur = nil
}

func (cur *Iterator) SeekFirst() ([]byte, []byte, bool) {
	key, val, err := cur.cur.GetVal(nil, nil, mdb.FIRST)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

func (cur *Iterator) SeekLast() ([]byte, []byte, bool) {
	key, val, err := cur.cur.GetVal(nil, nil, mdb.LAST)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

func (cur *Iterator) Previous() ([]byte, []byte, bool) {
	key, val, err := cur.cur.GetVal(nil, nil, mdb.PREV)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

func (cur *Iterator) Next() ([]byte, []byte, bool) {
	key, val, err := cur.cur.GetVal(nil, nil, mdb.NEXT)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

func (cur *Iterator) SeekTo(k []byte) ([]byte, []byte, bool) { // SET TODO: test behaviour
	key, val, err := cur.cur.GetVal(k, nil, mdb.SET_RANGE)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

func (cur *Iterator) Get() ([]byte, []byte, bool) { // GET_CURRENT
	key, val, err := cur.cur.GetVal(nil, nil, mdb.GET_CURRENT)
	// TODO: test & process err
	return key.BytesNoCopy(), val.BytesNoCopy(), (err == nil)
}

// TODO: test behaviour
func (cur *Iterator) Put(key, val []byte) {
	err := cur.cur.Put(key, val, 0)
	// TODO: test & process err
	if err != nil {
		panic(err)
	}
}

func (cur *Iterator) Delete() {
	err := cur.cur.Del(0)
	if err != nil {
		panic(err)
	}
}
