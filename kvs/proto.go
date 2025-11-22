package kvs

type PutRequest struct {
	TxID  TXID
	Key   string
	Value string
}

type PutResponse struct {
	TxID   TXID
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

type Waiter struct {
	TxID TXID
	Mode string
	Done chan struct{}
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
