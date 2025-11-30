package main
import(

	"github.com/rstutsman/cs6450-labs/kvs"

)

func (kv *KVService) acquireLockNoWait(lockState *LockState, txID kvs.TXID, mode string) error{
	//trying once

	if kv.canAcquire(lockState, txID, mode){
		return nil
	}
	//abort because someone else holds a conflicting lock
	return ErrAbort
}