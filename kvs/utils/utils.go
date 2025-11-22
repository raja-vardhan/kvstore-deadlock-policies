package utils

import (
	"fmt"
	"strings"

	"github.com/rstutsman/cs6450-labs/kvs"
)

func TXIDComparator(a, b interface{}) int {
	tx1 := a.(kvs.TXID)
	tx2 := b.(kvs.TXID)

	if tx1.Lo < tx2.Lo {
		return 1
	}

	if tx1.Lo > tx2.Lo {
		return -1
	}

	return 0
}

func GenerateRandomTransaction(workload *kvs.Workload, txLen int) *kvs.Transaction {

	newTxn := kvs.Transaction{}
	newTxn.Ops = append(newTxn.Ops, kvs.Operation{Type: kvs.OpType(kvs.OpBegin)})

	for i := 0; i < txLen; i++ {

		nextTranOp := kvs.Operation{}

		wlOp := workload.Next()

		if wlOp.IsRead {
			nextTranOp.Type = kvs.OpType(kvs.OpGet)
			nextTranOp.Key = fmt.Sprintf("%d", wlOp.Key)
		} else {
			nextTranOp.Type = kvs.OpType(kvs.OpPut)
			nextTranOp.Key = fmt.Sprintf("%d", wlOp.Key)
			nextTranOp.Value = strings.Repeat("x", 128)
		}

		newTxn.Ops = append(newTxn.Ops, nextTranOp)

	}

	newTxn.Ops = append(newTxn.Ops, kvs.Operation{Type: kvs.OpType(kvs.OpCommit)})

	return &newTxn

}
