package kvs

type PutRequest struct {
	Key   string
	Value string
	TxID  TXID
}

type PutResponse struct {
	Status TxStatus
}

type TxStatus int

const (
	TxOK TxStatus = iota
	TxAborted
)

type TXID struct {
	Hi uint64
	Lo uint64
}

type GetRequest struct {
	Key  string
	TxID TXID // Added this field to identify the transaction
}

type GetResponse struct {
	Value  string
	Status TxStatus
}

type ReqObj struct {
	Key   string // Key for the operation.
	Value string // Value for the operation
	IsGet bool   // True if this is a Get operation, false for Put.
}

type RespObj struct {
	Value string // Value for the operation
	IsGet bool   // True if this is a Get operation, false for Put.
}

type BatchRequest struct {
	Batch []ReqObj
}

type BatchResponse struct {
	Batch []RespObj
}
