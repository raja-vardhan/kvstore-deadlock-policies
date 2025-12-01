package kvs

// ---------
type TxStatus int

const (
	TxOK TxStatus = iota
	TxAborted
	TxPrepared
)

type TXID struct {
	Hi uint64
	Lo uint64
}

// --------------Types of operations and transaction------------
type OpType int

const (
	OpBegin OpType = iota
	OpGet
	OpPut
	OpCommit
	OpAbort
)

type Operation struct {
	Type  OpType
	Key   string
	Value string
}

type Transaction struct {
	TxID TXID
	Ops  []Operation
}

// ------------------------------------------------------------

// --------------Server RPC Request and Response---------------
type BeginRequest struct {
	TxID TXID
}

type BeginResponse struct {
}

type GetRequest struct {
	TxID TXID
	Key  string
}

type GetResponse struct {
	TxID   TXID
	Value  string
	Status TxStatus
}

type PutRequest struct {
	TxID  TXID
	Key   string
	Value string
}

type PutResponse struct {
	TxID   TXID
	Status TxStatus
}

type PrepareRequest struct {
	TxID TXID
}

type PrepareResponse struct {
	Status TxStatus
}

type CommitRequest struct {
	Flag bool
	TxID TXID
}

type CommitResponse struct {
	TxID TXID
}

type AbortRequest struct {
	Flag bool
	TxID TXID
}

type AbortResponse struct {
	TxID TXID
}
