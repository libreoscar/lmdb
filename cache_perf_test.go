package lmdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

const Bucket string = "Bucket"

type Maper interface {
	Get(txn *ReadWriteTxn, key []byte) ([]byte, bool)
	Put(txn *ReadWriteTxn, key, value []byte)
	Del(txn *ReadWriteTxn, key []byte)
	Flush(txn *ReadWriteTxn)
}

//-------------------------- MemMapper ----------------------------------------------------

type MemMapper map[string][]byte

func (m MemMapper) Get(txn *ReadWriteTxn, key []byte) (val []byte, b bool) {
	val, b = m[string(key)]
	return
}

func (m MemMapper) Put(txn *ReadWriteTxn, key, value []byte) {
	m[string(key)] = value
}

func (m MemMapper) Del(txn *ReadWriteTxn, key []byte) {
	delete(m, string(key))
}

func (m MemMapper) Flush(txn *ReadWriteTxn) {}

//-------------------------- ZeroCacheMapper -----------------------------------------------

type ZeroCacheMapper struct{}

func (m *ZeroCacheMapper) Get(txn *ReadWriteTxn, key []byte) ([]byte, bool) {
	return txn.GetNoCopy(Bucket, key)
}

func (m *ZeroCacheMapper) Put(txn *ReadWriteTxn, key, value []byte) {
	txn.Put(Bucket, key, value)
}

func (m *ZeroCacheMapper) Del(txn *ReadWriteTxn, key []byte) {
	txn.Delete(Bucket, key)
}

func (m *ZeroCacheMapper) Flush(txn *ReadWriteTxn) {}

//-------------------------- CachedMapper ---------------------------------------------------

type CachedMapper struct {
	// cache & deleted never contain the same key
	cache   map[string][]byte
	deleted map[string]bool
}

func (m *CachedMapper) Get(txn *ReadWriteTxn, key []byte) ([]byte, bool) {
	strkey := string(key)

	if m.deleted[strkey] {
		return nil, false
	}

	v, b := m.cache[strkey]
	if b {
		return v, true
	} else {
		return txn.GetNoCopy(Bucket, key)
	}
}

func (m *CachedMapper) Put(txn *ReadWriteTxn, key, value []byte) {
	strkey := string(key)

	delete(m.deleted, strkey)

	cp := make([]byte, len(value))
	copy(cp, value)
	m.cache[strkey] = cp
}

func (m *CachedMapper) Del(txn *ReadWriteTxn, key []byte) {
	strkey := string(key)
	delete(m.cache, strkey)
	m.deleted[strkey] = true
}

func (m *CachedMapper) Flush(txn *ReadWriteTxn) {
	for k, v := range m.cache {
		txn.Put(Bucket, []byte(k), v)
	}
	for k, _ := range m.deleted {
		txn.Delete(Bucket, []byte(k))
	}
}

//-------------------------- Benchmark ---------------------------------------------------

func benchmarkMapper(b *testing.B, mapper Maper) {
	path, _ := ioutil.TempDir("", "lmdb_test")
	defer os.RemoveAll(path)

	db, err := Open(path, []string{Bucket})
	defer db.Close()
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		db.TransactionalRW(func(txn *ReadWriteTxn) error {
			for n := 0; n < 10000; n++ {
				// 4 read, 2 put, 1 delete
				key := []byte(fmt.Sprintf("%d", n%100))
				value := []byte(fmt.Sprintf("%d", n))

				k := n % 7
				if k < 4 { // get
					mapper.Get(txn, key)
				} else if k < 6 { // put
					mapper.Put(txn, key, value)
				} else { // delete
					mapper.Del(txn, key)
				}
			}
			mapper.Flush(txn)
			// commit all
			return nil
		})
	}
}

func BenchmarkNoCache(b *testing.B) {
	benchmarkMapper(b, &ZeroCacheMapper{})
}

func BenchmarkCached(b *testing.B) {
	mapper := &CachedMapper{
		cache:   make(map[string][]byte),
		deleted: make(map[string]bool),
	}
	benchmarkMapper(b, mapper)
}

func BenchmarkMemMapper(b *testing.B) {
	benchmarkMapper(b, make(MemMapper))
}
