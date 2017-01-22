package lmdb

import (
	"errors"
	"fmt"
	mdb "github.com/libreoscar/gomdb"
	//"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/store/localstore/engine"
	"log"
)

// Thread Safety
// 1) NOTLS mode is used exclusively, which allows read txns to freely migrate across
//    threads and for a single thread to maintain multiple read txns. This enables mostly
//    care-free use of read txns.
// 2) Most objects can be safely called by a single caller from a single thread, and usually it
//    only makes sense to have a single caller, except in the case of Database.
// 3) Most Database methods are thread-safe, and may be called concurrently, except for
//    Database.Close().
// 4) A write txn may only be used from the thread it was created on.
// 5) A read-only txn can move across threads, but it cannot be used concurrently from multiple
//    threads.
// 6) Iterator is not thread-safe, but it does not make sense to use it on any thread except the
//    thread that currently owns its associated txn.
//
//----------------------------------------------------------------------------------------
//
// Best practice:
// 1) Use iterators only in the txn that they are created
// 2) DO NOT modify the memory slice from GetNoCopy
// 3) Make sure all read/write txns are finished before Database.Close().

const (
	// There is no penalty for making this huge.
	// If you are on a 32-bit system, use Open2 and specify a smaller map size.
	MAP_SIZE_DEFAULT uint64 = 1 * 1024 * 1024 * 1024 * 1024 // 1TB

	// http://www.openldap.org/lists/openldap-technical/201305/msg00176.html
	MAX_DB_DEFAULT int = 32
)

type RWTxnCreator interface {
	TransactionalRW(func(*ReadWriteTxn) error) error
}

type dryRunDummyError struct{}

func (d dryRunDummyError) Error() string {
	return "Dummy error of dry running a db transaction."
}

// Dry run the db transaction, i.e. always rollback, even if the returned error is nil.
func DryRunRWTxn(rwtxner RWTxnCreator, f func(*ReadWriteTxn) error) error {
	err := rwtxner.TransactionalRW(func(rwtx *ReadWriteTxn) error {
		err := f(rwtx)
		if err == nil {
			err = dryRunDummyError{}
		}
		return err
	})

	if _, ok := err.(dryRunDummyError); ok {
		err = nil
	}
	return err
}

// a make-patch is a dry-run with a patch as its return value
func MakePatch(rwtxner RWTxnCreator, f func(*ReadWriteTxn) error) (patch TxnPatch, err error) {
	rwtxner.TransactionalRW(func(rwtxn *ReadWriteTxn) error {
		origin := rwtxn.dirtyKeys
		rwtxn.dirtyKeys = make(map[string]bool)
		err = f(rwtxn)
		if err == nil {
			for serializedCellKey := range rwtxn.dirtyKeys {
				cellKey, err2 := DeserializeCellKey(serializedCellKey)
				if err2 != nil {
					panic(fmt.Errorf("deserialization error: %s, serialized key = %v",
						err2.Error(), serializedCellKey))
				}
				cell := cellState{bucket: cellKey.Bucket, key: cellKey.Key}
				cell.value, cell.exists = rwtxn.Get(cellKey.Bucket, cellKey.Key)
				patch = append(patch, cell)
			}
			err = dryRunDummyError{}
		}
		rwtxn.dirtyKeys = origin
		return err
	})

	if _, ok := err.(dryRunDummyError); ok {
		err = nil
	}
	return
}

// run with a patch as its return value
// In MakePatch cell.exists == false  means deleted, here we reset this field and cell.exists==falsemeans the cell is jast created

func MakeDePatch(rwtxner RWTxnCreator, f func(*ReadWriteTxn) error) (TxnPatch, error) {
	pa, err := MakePatch(rwtxner, f)
	//	fmt.Println(pa)
	if err != nil {
		return nil, err
	}
	rwtxner.TransactionalRW(func(rwtxn *ReadWriteTxn) error {
		for i, j := range pa {
			/*value = */
			value, err := rwtxn.Get(j.bucket, j.key)
			if err == false {
				//this cell is just created
				//fmt.Println(j)
				pa[i].exists = false
				// j.exists = false
			} else {
				pa[i].exists = true
				j.value = value
			}
		}

		return f(rwtxn)
	})

	return pa, err

}

func GetCellKey(x cellState) []byte {
	return x.key
}

//--------------------------------- Database ---------------------------------------------

type Database struct {
	env *mdb.Env
	// In this package, a DBI is obtained only through Open/Open2, and is never closed until
	// Context.Close(), in which all dbis are closed automatically.
	buckets map[string]mdb.DBI
}

type Stat mdb.Stat
type Info mdb.Info

func Version() string {
	return mdb.Version()
}

func Open(path string, buckets []string) (*Database, error) {
	return Open2(path, buckets, MAP_SIZE_DEFAULT, MAX_DB_DEFAULT)
}

func Open2(path string, buckets []string, maxMapSize uint64, maxDB int) (db *Database, err error) {
	if maxDB < len(buckets) {
		maxDB = len(buckets)
	}

	// TODO: (Potential bug):
	// From mdb_env_open's doc,
	//  "If this function fails, #mdb_env_close() must be called to discard the #MDB_env handle."
	// But mdb.NewEnv doesnot call mdb_env_close() when it fails, AND it just return nil as env.
	// Patch gomdb if this turns out to be a big issue.
	env, err := mdb.NewEnv()
	db = &Database{env, make(map[string]mdb.DBI)}
	defer func() {
		if err != nil && env != nil {
			log.Printf("[ERROR] Open db failed. %v", err)
			env.Close()
			db.env = nil
		}
	}()
	if err != nil {
		return
	}

	err = env.SetMapSize(maxMapSize)
	if err != nil {
		return
	}

	err = env.SetMaxDBs(mdb.DBI(maxDB))
	if err != nil {
		return
	}

	MDB_NOTLS := uint(0x200000)
	MDB_NORDAHEAD := uint(0x800000)
	err = env.Open(path, MDB_NOTLS|MDB_NORDAHEAD, 0664)
	if err != nil {
		return
	}

	err = db.openBuckets(buckets)
	return
}

func (db *Database) openBuckets(buckets []string) error {
	return db.TransactionalRW(func(txn *ReadWriteTxn) error {
		for _, name := range buckets {
			if name == "" {
				return errors.New("Bucket name is empty")
			}

			_, exist := db.buckets[name]
			if exist {
				continue
			}

			dbi, err := txn.txn.DBIOpen(&name, mdb.CREATE)
			if err != nil {
				return err
			} else {
				db.buckets[name] = dbi
			}
		}
		return nil
	})
}

func (db *Database) GetExistingBuckets() (buckets []string, err error) {
	db.TransactionalRW(func(txn *ReadWriteTxn) error {
		dbi, err := txn.txn.DBIOpen(nil, mdb.CREATE)
		if err != nil {
			return err
		}

		cur, err := txn.txn.CursorOpen(dbi)
		if err != nil {
			return err
		}

		itr := (*Iterator)(cur)
		defer itr.Close()
		if !itr.SeekFirst() {
			return nil
		}

		for {
			key, _ := itr.GetNoCopy()
			buckets = append(buckets, string(key))

			if !itr.Next() {
				break
			}
		}
		return nil
	})
	return
}

func (db *Database) Close() error {
	if db.env != nil {
		return db.env.Close() // all opened dbis are closed during this process
	}
	return nil
}

func (db *Database) Stat() *Stat {
	stat, err := db.env.Stat()
	if err != nil { // Possible errors: EINVAL
		panic(err)
	}
	return (*Stat)(stat)
}

func (db *Database) Info() *Info {
	info, err := db.env.Info()
	if err != nil { // error when env == nil, so panic
		panic(err)
	}
	return (*Info)(info)
}

func (db *Database) TransactionalR(f func(ReadTxner)) {
	txn, err := db.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}

	var panicF interface{} // panic from f
	rdTxn := ReadTxn{db.buckets, txn, nil}

	defer func() {
		for _, itr := range rdTxn.itrs {
			itr.Close() // no panic
		}
		rdTxn.itrs = nil

		txn.Abort()
		if panicF != nil {
			panic(panicF) // re-panic
		}
	}()

	func() {
		defer func() {
			panicF = recover()
		}()
		f(&rdTxn)
	}()
}

func (db *Database) TransactionalRW(f func(*ReadWriteTxn) error) (err error) {
	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil { // Possible Errors: MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, ENOMEM
		panic(err)
	}

	var panicF interface{} // panic from f
	rwCtx := ReadWriteTxn{db.env, &ReadTxn{db.buckets, txn, nil}, nil}

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

type MyDriver struct {
}

func (d MyDriver) Open(path string) (engine.DB, error) {
	return (Open(path, []string{" "}))
}

func (db *Database) Get(key []byte) ([]byte, error) {
	var res []byte
	var err error = nil
	db.TransactionalR(func(txn ReadTxner) {
		res, _ = txn.Get(" ", key)
	})
	return res, err
}

func (db *Database) Seek(key []byte) ([]byte, []byte, error) {
	var rkey, rval []byte
	var rerr error
	db.TransactionalR(func(txn ReadTxner) {
		itr := txn.Iterate(" ")
		if key == nil {
			err := itr.SeekFirst()
			if err == false {
				rkey = nil
				rval = nil
				rerr = engine.ErrNotFound
				return
			}
			rkey, rval = itr.Get()
			rerr = nil
			return
		}
		err := itr.SeekGE(key)
		if err == false {
			rkey = nil
			rval = nil
			rerr = engine.ErrNotFound
			return
		}
		rkey, rval = itr.Get()
		rerr = nil
	})
	return rkey, rval, rerr
}

func (db *Database) SeekReverse(key []byte) ([]byte, []byte, error) {
	var rkey, rval []byte
	var rerr error
	db.TransactionalR(func(txn ReadTxner) {
		itr := txn.Iterate(" ")
		if key == nil {
			err := itr.SeekLast()
			if err == false {
				rkey = nil
				rval = nil
				rerr = engine.ErrNotFound
				return
			}
			rkey, rval = itr.Get()
			rerr = nil
			return
		}

		err := itr.SeekGE(key)
		if err == false {
			err1 := itr.SeekLast()
			if err1 == false {
				rkey = nil
				rval = nil
				rerr = engine.ErrNotFound
				return
			}
			rkey, rval = itr.Get()
			rerr = nil
			return
		} else {
			err2 := itr.Prev()
			if err2 == false {
				rkey = nil
				rval = nil
				rerr = engine.ErrNotFound
				return
			}
			rkey, rval = itr.Get()
			rerr = nil
			return
		}
	})
	return rkey, rval, rerr

}

type Batch struct {
	patch TxnPatch
}

func (b *Batch) Put(key []byte, value []byte) {
	cell := cellState{bucket: " ", key: key, exists: true, value: value}
	b.patch = append(b.patch, cell)
	return
}

func (b *Batch) Delete(key []byte) {
	cell := cellState{bucket: " ", key: key, exists: false}
	b.patch = append(b.patch, cell)
	return
}

func (b *Batch) Len() int {
	return len(b.patch)
}
func (db *Database) NewBatch() engine.Batch {
	return &Batch{}
}

func (db *Database) Commit(b engine.Batch) error {
	db.TransactionalRW(func(txn *ReadWriteTxn) error {
		pc, _ := b.(*Batch)
		return txn.ApplyPatch(pc.patch)
	})
	return nil
}

func (db *Database) PrintAll() {
	db.TransactionalR(func(txn ReadTxner) {
		itr := txn.Iterate(" ")
		for {
			rkey, rval := itr.Get()
			fmt.Println(rkey, rval)
			err := itr.Next()
			if err == false {
				break
			}
		}

	})
}
