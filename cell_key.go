package lmdb

import "encoding/json"

type CellKey struct {
	Bucket string
	Key    []byte
}

func (ck CellKey) Serialize() string {
	b, err := json.Marshal(ck)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func DeserializeCellKey(s string) (rst CellKey) {
	err := json.Unmarshal([]byte(s), &rst)
	if err != nil {
		panic(err)
	}
	return
}
