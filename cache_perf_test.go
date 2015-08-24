package lmdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

const Bucket string = "Bucket"

type Maper interface {
	Get(ctx *Context, key []byte) ([]byte, bool)
	Put(ctx *Context, key, value []byte)
	Del(ctx *Context, key []byte)
	Flush(ctx *Context)
}

//-------------------------- MemMapper ----------------------------------------------------

type MemMapper map[string][]byte

func (m MemMapper) Get(ctx *Context, key []byte) (val []byte, b bool) {
	val, b = m[string(key)]
	return
}

func (m MemMapper) Put(ctx *Context, key, value []byte) {
	m[string(key)] = value
}

func (m MemMapper) Del(ctx *Context, key []byte) {
	delete(m, string(key))
}

func (m MemMapper) Flush(ctx *Context) {}

//-------------------------- ZeroCacheMapper -----------------------------------------------

type ZeroCacheMapper struct{}

func (m *ZeroCacheMapper) Get(ctx *Context, key []byte) ([]byte, bool) {
	return ctx.GetNoCopy(Bucket, key)
}

func (m *ZeroCacheMapper) Put(ctx *Context, key, value []byte) {
	ctx.Put(Bucket, key, value)
}

func (m *ZeroCacheMapper) Del(ctx *Context, key []byte) {
	ctx.Delete(Bucket, key)
}

func (m *ZeroCacheMapper) Flush(ctx *Context) {}

//-------------------------- CachedMapper ---------------------------------------------------

type CachedMapper struct {
	// cache & deleted never contain the same key
	cache   map[string][]byte
	deleted map[string]bool
}

func (m *CachedMapper) Get(ctx *Context, key []byte) ([]byte, bool) {
	strkey := string(key)

	if m.deleted[strkey] {
		return nil, false
	}

	v, b := m.cache[strkey]
	if b {
		return v, true
	} else {
		return ctx.GetNoCopy(Bucket, key)
	}
}

func (m *CachedMapper) Put(ctx *Context, key, value []byte) {
	strkey := string(key)

	delete(m.deleted, strkey)

	cp := make([]byte, len(value))
	copy(cp, value)
	m.cache[strkey] = cp
}

func (m *CachedMapper) Del(ctx *Context, key []byte) {
	strkey := string(key)
	delete(m.cache, strkey)
	m.deleted[strkey] = true
}

func (m *CachedMapper) Flush(ctx *Context) {
	for k, v := range m.cache {
		ctx.Put(Bucket, []byte(k), v)
	}
	for k, _ := range m.deleted {
		ctx.Delete(Bucket, []byte(k))
	}
}

//-------------------------- Benchmark ---------------------------------------------------

func benchmarkMapper(b *testing.B, mapper Maper) {
	path, _ := ioutil.TempDir("", "lmdb_test")
	defer os.RemoveAll(path)

	ctx, err := Open(path, []string{Bucket})
	defer ctx.Close()

	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		ctx.Transactional(true, func(ctx *Context) error {
			for n := 0; n < 10000; n++ {
				// 4 read, 2 put, 1 delete
				key := []byte(fmt.Sprintf("%d", n%100))
				value := []byte(fmt.Sprintf("%d", n))

				k := n % 7
				if k < 4 { // get
					mapper.Get(ctx, key)
				} else if k < 6 { // put
					mapper.Put(ctx, key, value)
				} else { // delete
					mapper.Del(ctx, key)
				}
			}
			mapper.Flush(ctx)
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
