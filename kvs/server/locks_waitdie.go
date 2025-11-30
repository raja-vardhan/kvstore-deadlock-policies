package main
import(
	"fmt"

	"github.com/rstutsman/cs6450-labs/kvs"

)

//implementation of the wait die policy for one key
func (kv *KVService) acquireLockWaitDie(lockState *LockState, txID kvs.TXID, mode string) error{
	for{
		if kv.canAcquire(lockState, txID, mode){
			return nil
		}
		younger := false

		if mode =="S"{
			if lockState.writer != (kvs.TXID{}) && lockState.writer != txID{
				if !kv.isOlder(txID, lockState.writer){
					younger=true
				}
			}
		}else{ //mode X
			if lockState.writer != (kvs.TXID{}) && lockState.writer != txID{
				if !kv.isOlder(txID, lockState.writer){
					younger= true
				}
			}
			for reader := range lockState.readers{
				if reader != txID && !kv.isOlder(txID, reader){
					younger=true
					break
				}
			}
		}
		if younger{
			fmt.Printf("[WAITDIE] tx %v is YOUNGER, aborting (mode=%s)\n", txID, mode)
			return ErrAbort
		}

		//whatever reaches this is older than all of the conflicting holders
		//wait
		fmt.Printf("[WAITDIE] tx %v is OLDER, going to WAIT (mode=%s)\n", txID, mode)
		w:= Waiter{
			txID: txID,
			done: make(chan struct{}),
		}
		lockState.waitQueue.Enqueue(w)
		//release the global mutex so other transactions can progress
		kv.mu.Unlock()
		<-w.done
		kv.mu.Lock() //retry the loop
	}

}