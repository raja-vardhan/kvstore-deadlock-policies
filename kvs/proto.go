package kvs

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
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
