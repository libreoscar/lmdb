package lmdb

type cellState struct {
	bucket string
	key    []byte
	exists bool
	value []byte
}

type TxPatch []cellState
