package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/rpc"
	"sync"
	"time"

	"github.com/emirpasic/gods/queues/priorityqueue"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/rstutsman/cs6450-labs/kvs"
)

var LOCK_DENIED = "LOCK_DENIED"
var TXN_NOT_FOUND = "TXN_NOT_FOUND"
var TXN_ABORTED = "TXN_ABORTED"

type Waiter struct {
	txID kvs.TXID
	done chan struct{}
}

func TXIDComparator(a, b interface{}) int {
	tx1 := a.(*Waiter)
	tx2 := b.(*Waiter)

	if tx1.txID.Hi < tx2.txID.Hi {
		return -1
	}

	if tx1.txID.Hi > tx2.txID.Hi {
		return 1
	}

	if tx1.txID.Lo < tx2.txID.Lo {
		return -1
	}

	if tx1.txID.Lo > tx2.txID.Lo {
		return 1
	}

	if tx1.txID.ID < tx2.txID.ID {
		return -1
	}

	if tx1.txID.ID > tx2.txID.ID {
		return 1
	}

	return 0
}

type TxRecord struct {
	id       kvs.TXID
	status   kvs.TxStatus
	writeSet map[string]string // staged writes
	readSet  map[string]string // optional
}

type LockState struct {
	readers   map[kvs.TXID]struct{} // S holders
	writer    kvs.TXID              // X holder
	waitQueue *priorityqueue.Queue  // Waiting transactions
}

type Stats struct {
	commits uint64
	aborts  uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.commits = s.commits - prev.commits
	r.aborts = s.aborts - prev.aborts
	return r
}

type KVService struct {
	mu        sync.Mutex
	mp        cmap.ConcurrentMap[string, string]
	lockTable map[string]*LockState
	txTable   map[kvs.TXID]*TxRecord
	stats     Stats
	prevStats Stats
	lastPrint time.Time
	policy Policy
}

func NewKVService() *KVService {
	kvService := &KVService{}
	kvService.mp = cmap.New[string]()
	kvService.lockTable = make(map[string]*LockState)
	kvService.txTable = make(map[kvs.TXID]*TxRecord)
	kvService.lastPrint = time.Now()
	return kvService
}

// Is a older than b?
func isOlder(a kvs.TXID, b kvs.TXID) bool {
	if a.Hi != b.Hi {
		return a.Hi < b.Hi
	}
	if a.Lo != b.Lo {
		return a.Lo < b.Lo
	}

	return a.ID < b.ID
}

func (kv *KVService) createLockStateIfAbsent(key string) *LockState {
	if _, ok := kv.lockTable[key]; !ok {
		lockState := &LockState{
			readers:   make(map[kvs.TXID]struct{}),
			waitQueue: priorityqueue.NewWith(TXIDComparator),
		}
		kv.lockTable[key] = lockState
	}

	return kv.lockTable[key]

}
/*
func canAcquire(lockState *LockState, txRecord *TxRecord, mode string) bool {

	if mode == "S" {

		// Try to acquire S-lock. Deny only if another transaction is writing to it
		if lockState.writer != (kvs.TXID{}) && lockState.writer != txRecord.id {
			return false
		}

		return true

	} else {

		// Try to acquire X-lock. Deny if another transaction is writing to it or reading from it
		if lockState.writer != (kvs.TXID{}) && lockState.writer != txRecord.id {
			return false
		}

		for reader, _ := range lockState.readers {
			if reader != txRecord.id {
				return false
			}
		}

		return true
	}
}
*/
/*
func (kv *KVService) woundTransaction(txID kvs.TXID) {

	if txRecord, ok := kv.txTable[txID]; !ok || txRecord.status == kvs.TxAborted {
		return
	} else {

		txRecord.status = kvs.TxAborted

		kv.releaseLock(txID)
	}

}

func (kv *KVService) acquireLock(key string, txID kvs.TXID, mode string) bool {

	for {

		txRecord, ok := kv.txTable[txID]
		if !ok || txRecord.status == kvs.TxAborted {
			fmt.Println("transaction ", txID, " waiting on ", key, "for ", mode, " was pre-empted")
			kv.wakeNext(key)
			return false
		}

		lockState := kv.lockTable[key]

		// Try to acquire the S/X lock
		if canAcquire(lockState, txRecord, mode) {
			if mode == "S" {
				lockState.readers[txRecord.id] = struct{}{}
			} else {
				lockState.writer = txRecord.id
			}
			return true
		}

		// Remove transactions with a younger timestamp
		if isOlder(txRecord.id, lockState.writer) && kv.txTable[lockState.writer].status != kvs.TxPrepared {
			kv.woundTransaction(lockState.writer)
		}
		for reader, _ := range lockState.readers {
			if isOlder(txRecord.id, reader) && kv.txTable[reader].status != kvs.TxPrepared {
				kv.woundTransaction(reader)
			}
		}

		// Try to acquire the S/X lock again
		if canAcquire(lockState, txRecord, mode) {
			if mode == "S" {
				lockState.readers[txRecord.id] = struct{}{}
			} else {
				lockState.writer = txRecord.id
			}
			return true
		}

		// Wait otherwise for a signal
		w := &Waiter{txID: txRecord.id, done: make(chan struct{})}
		lockState.waitQueue.Enqueue(w)
		// Release mutex

		kv.mu.Unlock()
		fmt.Println(txID, " sleeping")

		<-w.done

		// Acquire mutex
		kv.mu.Lock()
		fmt.Println(txID, " woken up")
	}

}
*/
/*
func (kv *KVService) wakeNext(key string) {
	lockState := kv.lockTable[key]
	for !lockState.waitQueue.Empty() {
		wRaw, _ := lockState.waitQueue.Dequeue()
		w := wRaw.(*Waiter)

		if txRecord, ok := kv.txTable[w.txID]; !ok || txRecord.status == kvs.TxAborted {
			// Transaction already aborted; wake the goroutine so it can see that.
			close(w.done)
			// And continue to look for the next valid waiter
			continue
		}

		// Valid active transaction
		close(w.done)
		return
	}
}

func (kv *KVService) releaseLock(txID kvs.TXID) {

	txRecord := kv.txTable[txID]

	for key := range txRecord.readSet {
		lockState := kv.lockTable[key]
		delete(lockState.readers, txRecord.id)
		kv.wakeNext(key)
	}

	for key := range txRecord.writeSet {
		lockState := kv.lockTable[key]
		if lockState.writer == txRecord.id {
			lockState.writer = kvs.TXID{}
		}
		kv.wakeNext(key)
	}
}
*/
func (kv *KVService) Begin(request *kvs.BeginRequest, response *kvs.BeginResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.txTable[request.TxID]; !ok {
		txRecord := &TxRecord{
			id:       request.TxID,
			status:   kvs.TxOK,
			writeSet: make(map[string]string),
			readSet:  make(map[string]string),
		}

		kv.txTable[request.TxID] = txRecord
	}

	return nil
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("Get", request)

	// Check that the transaction has not been aborted
	txRecord := kv.txTable[request.TxID]
	if txRecord.status == kvs.TxAborted {
		return errors.New(TXN_ABORTED)
	}

	// Check if the transaction has already written to this key (already holds the X-lock)
	// Read your own writes
	// or if it has read the key before
	if val, ok := txRecord.writeSet[request.Key]; ok {
		response.Value = val
		return nil
	}
	if val, ok := txRecord.readSet[request.Key]; ok {
		response.Value = val
		return nil
	}
	lockState := kv.createLockStateIfAbsent(request.Key)

    if err := kv.acquireLock(lockState, request.TxID, "S"); err != nil{
        if errors.Is(err, ErrAbort){
            return errors.New(TXN_ABORTED)
        }
        return err
    }

    val, _ := kv.mp.Get(request.Key)
    txRecord.readSet[request.Key] = val
    response.Value=val
	/*
	// Check if lockState for the key exists, if not create it
	kv.createLockStateIfAbsent(request.Key)

	// Try to acquire S-lock, update read set of the transaction
	if kv.acquireLock(request.Key, request.TxID, "S") {
		val, _ := kv.mp.Get(request.Key)
		txRecord.readSet[request.Key] = val
		response.Value = val
	} else {
		return errors.New(TXN_ABORTED)
	}
*/
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("Put", request)

	// Check if the transaction record exists
	txRecord := kv.txTable[request.TxID]
	if txRecord.status == kvs.TxAborted {
		return errors.New(TXN_ABORTED)
	}

	// Check if the transaction has already written to this key (already holds the X-lock)
	if _, ok := txRecord.writeSet[request.Key]; ok {
		txRecord.writeSet[request.Key] = request.Value
		return nil
	}

	lockState := kv.createLockStateIfAbsent(request.Key)
    if err := kv.acquireLock(lockState, request.TxID, "X"); err != nil{
        if errors.Is(err, ErrAbort){
            return errors.New(TXN_ABORTED)
        }
        return err
    }
    txRecord.writeSet[request.Key]=request.Value
	/*
	// Check if lockState for the key exists, if not create it
	kv.createLockStateIfAbsent(request.Key)

	// Try to acquire X-lock and update write set of the transaction
	if kv.acquireLock(request.Key, request.TxID, "X") {
		txRecord.writeSet[request.Key] = request.Value
	} else {
		return errors.New(TXN_ABORTED)
	}
	*/
	return nil

}

func (kv *KVService) Prepare(request *kvs.PrepareRequest, response *kvs.PrepareResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("Prepare", request)

	// Check if the transaction record exists
	txRecord := kv.txTable[request.TxID]
	if txRecord.status == kvs.TxAborted {
		fmt.Println("Transaction preempted, prepare failed ", request.TxID)
		response.Status = kvs.TxAborted
		return errors.New(TXN_NOT_FOUND)
	}

	txRecord.status = kvs.TxPrepared
	response.Status = kvs.TxOK

	return nil
}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("Commit", request)

	txRecord := kv.txTable[request.TxID]

	for key := range txRecord.readSet{
        if lockState, ok := kv.lockTable[key]; ok{
            kv.releaseLock(lockState, txRecord.id)
        }
    }
    for key := range txRecord.writeSet{
        if lockState, ok := kv.lockTable[key]; ok{
            kv.releaseLock(lockState, txRecord.id)
        }
    }

	/*
	// Do writes
	for key, val := range txRecord.writeSet {
		kv.mp.Set(key, val)
	}

	// Release locks held by the transaction
	kv.releaseLock(txRecord.id)
	*/
	// Remove transaction from table
	delete(kv.txTable, request.TxID)

	if request.Flag {
		kv.stats.commits++
	}

	fmt.Println("Commiting request ", request)

	return nil
}

func (kv *KVService) Abort(request *kvs.AbortRequest, response *kvs.AbortResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// fmt.Println("Abort ", request)

	// Check if the transaction record exists
	txRecord := kv.txTable[request.TxID]

	for key := range txRecord.readSet{
        if lockState, ok := kv.lockTable[key]; ok{
            kv.releaseLock(lockState, txRecord.id)
        }
    }
    for key := range txRecord.writeSet{
        if lockState, ok := kv.lockTable[key]; ok{
            kv.releaseLock(lockState, txRecord.id)
        }
    }

	// Release locks held by the transaction
	//kv.releaseLock(txRecord.id)

	// Remove transaction from table
	delete(kv.txTable, request.TxID)

	if request.Flag {
		kv.stats.aborts++
	}

	return nil
}

func (kv *KVService) printStats() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	stats := Stats{
		commits: kv.stats.commits,
		aborts:  kv.stats.aborts,
	}

	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("commits/s %0.2f\naborts/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.commits)/deltaS,
		float64(diff.aborts)/deltaS,
		float64(diff.commits+diff.aborts)/deltaS)
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	port := flag.String("port", "8080", "Port to run the server on")
	policyFlag := flag.String("policy", "nowait", "Lock policy: nowait|waitdie")
	flag.Parse()

	kvs := NewKVService()
	kvs.policy= ParsePolicy(*policyFlag)
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	//fmt.Printf("Starting KVS server on :%s\n", *port)

	fmt.Printf("Starting KVS server on :%s (policy=%s)\n", *port, kvs.policy.String())

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}