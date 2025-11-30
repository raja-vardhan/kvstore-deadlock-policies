package main
import(
	"errors"
	"fmt"
	"strings"
	"github.com/rstutsman/cs6450-labs/kvs"

)

type Policy int 

const(
	PolicyNoWait Policy= iota
	PolicyWaitDie
	//PolicyWoundWait
)

//choose the policy at startup with a "-policy" flag
func ParsePolicy(s string) Policy{
	switch strings.ToLower(s){
	case "waitdie", "wait-die":
		return PolicyWaitDie
	default:
		return PolicyNoWait
	}
}
//choose the policy at startup with a "-policy" flag
func (p Policy) String() string{
	switch p{
	case PolicyWaitDie:
		return "waitdie"

	default:
		return "nowait"
	}
}

//error to tell us "this transaction should abort under the policy..." delete? change?
var ErrAbort= errors.New("abort under policy- ABORT")


//age comparison for wait die
//txid.lo is the timestamp
func (kv *KVService) isOlder(a, b kvs.TXID) bool{
	//empty id
	if b==(kvs.TXID{}){
		return false
	}
	return a.Lo < b.Lo
}

//moved from main.go to locks.go - this will try to grant a S or X lock
func (kv *KVService) canAcquire(lockState *LockState, txID kvs.TXID, mode string) bool {

	if mode == "S" {

		// Try to acquire S-lock. Deny only if another transaction is writing to it
		if lockState.writer != (kvs.TXID{}) && lockState.writer != txID {
			return false
		}

		lockState.readers[txID] = struct{}{}
		return true

	} 
	//mode== x 
	if lockState.writer != (kvs.TXID{}) && lockState.writer != txID{
		return false
	}
	for reader := range lockState.readers{
		if reader != txID{
			return false
		}
	}
	lockState.writer= txID
	return true
}

//the wait die sleep retry loop
func (kv *KVService) acquireLock(lockState *LockState, txID kvs.TXID, mode string) error{

	switch kv.policy{
	case PolicyNoWait:
		return kv.acquireLockNoWait(lockState,txID,mode)
	case PolicyWaitDie:
		return kv.acquireLockWaitDie(lockState, txID, mode)

	default: //fallback to no wait
		return kv.acquireLockNoWait(lockState, txID, mode)
	}
}




func (kv *KVService) releaseLock(lockState *LockState, txID kvs.TXID) {

	delete(lockState.readers, txID)
	if lockState.writer == txID {
		lockState.writer = kvs.TXID{}
	}

	//wake a waiting transaction to make progress
	if lockState.waitQueue != nil && !lockState.waitQueue.Empty(){
		v, _ := lockState.waitQueue.Dequeue() // Dequeue comes from "github.com/emirpasic/gods/queues/priorityqueue"
		waiter := v.(Waiter)
		fmt.Printf("[RELEASE] tx %v released; waking waiter tx %v (queue_len_after=%d)\n",
            txID, waiter.txID, lockState.waitQueue.Size())
		close(waiter.done)
	}
	// Ensure that atleast one transaction can progress

}








