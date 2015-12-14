package lmdb

import (
	"reflect"
)

func MakePatchOfDb(db *Database) (rst TxnPatch) {
	buckets, err := db.GetExistingBuckets()
	if err != nil {
		panic(err)
	}

	db.TransactionalR(func(txn ReadTxner) {
		for _, bucket := range buckets {
			itr := txn.Iterate(bucket)
			if itr == nil {
				continue
			}

			for {
				key, val := itr.Get()
				rst = append(rst, cellState{bucket, key, true, val})
				if !itr.Next() {
					break
				}
			}
		}
	})
	return
}

func IsEqualDb(db1 *Database, db2 *Database) bool {
	patch1 := MakePatchOfDb(db1)
	patch2 := MakePatchOfDb(db2)
	return reflect.DeepEqual(patch1, patch2)
}
