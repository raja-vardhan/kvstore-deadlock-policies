package utils

import (
	"github.com/rstutsman/cs6450-labs/kvs"
)

func TXIDComparator(a, b interface{}) int {
	tx1 := a.(*kvs.Waiter).TxID
	tx2 := b.(*kvs.Waiter).TxID

	if tx1.Lo < tx2.Lo {
		return 1
	}

	if tx1.Lo > tx2.Lo {
		return -1
	}

	return 0
}
