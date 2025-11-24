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

type Waiter struct {
	txID kvs.TXID
	done chan struct{}
}

func TXIDComparator(a, b interface{}) int {
	tx1 := a.(Waiter)
	tx2 := b.(Waiter)

	if tx1.txID.Lo < tx2.txID.Lo {
		return 1
	}

	if tx1.txID.Lo > tx2.txID.Lo {
		return -1
	}

	return 0
}

type TxRecord struct {
	id       kvs.TXID
	status   kvs.TxStatus
	writeSet map[string]string   // staged writes
	readSet  map[string]struct{} // optional
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
	// 16 mutexes
	mu        sync.Mutex
	mp        cmap.ConcurrentMap[string, string]
	lockTable map[string]*LockState
	txTable   map[kvs.TXID]*TxRecord
	stats     Stats
	prevStats Stats
	lastPrint time.Time
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
	return a.Lo < b.Lo
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

func canAcquire(key string, lockState *LockState, txRecord TxRecord, mode string) bool {

	if mode == "S" {

		// Try to acquire S-lock. Deny only if another transaction is writing to it
		if lockState.writer != (kvs.TXID{}) && lockState.writer != txRecord.id {
			return false
		}

		lockState.readers[txRecord.id] = struct{}{}

	} else {

		// Try to acquire X-lock. Deny if another transaction is writing to it or reading from it
		if lockState.writer != (kvs.TXID{}) {
			if lockState.writer != txRecord.id {
				return false
			}
		}

		for reader, _ := range lockState.readers {
			if reader != txRecord.id {
				return false
			}
		}

		lockState.writer = txRecord.id
	}

	return true
}

func markTransactionInactive(txID *kvs.TXID, lockState *LockState, txTable *map[kvs.TXID]*TxRecord) {

	delete(*txTable, *txID)
	if lockState.writer == *txID {
		lockState.writer = kvs.TXID{}
	}
	delete(lockState.readers, *txID)
}

func (kv *KVService) acquireLock(key string, lockState *LockState, txRecord TxRecord, mode string) {

	for {

		// Try to acquire the S/X lock
		if canAcquire(key, lockState, txRecord, mode) {
			return
		}

		// Remove transactions with a younger timestamp
		if isOlder(txRecord.id, lockState.writer) {
			markTransactionInactive(&lockState.writer, lockState, &kv.txTable)
		}
		for reader, _ := range lockState.readers {
			if isOlder(txRecord.id, reader) {
				markTransactionInactive(&reader, lockState, &kv.txTable)
			}
		}

		// Try to acquire the S/X lock again
		if canAcquire(key, lockState, txRecord, mode) {
			return
		}

		// Wait otherwise for a signal
		w := Waiter{txID: txRecord.id, done: make(chan struct{})}
		lockState.waitQueue.Enqueue(w)
		// Release mutex

		kv.mu.Unlock()

		<-w.done

		// Acquire mutex
		kv.mu.Lock()
	}

}

// Wake the next waiting transaction (if any)
func (kv *KVService) wakeNext(lockState *LockState) {
	if lockState.waitQueue.Empty() {
		return
	}

	// Pop the highest-priority waiter (oldest TX)
	wRaw, _ := lockState.waitQueue.Dequeue()
	w := wRaw.(Waiter)

	// Skip aborted transactions
	if _, ok := kv.txTable[w.txID]; !ok {
		// recursively wake next (this one is dead)
		kv.wakeNext(lockState)
		return
	}

	// Wake one waiter
	close(w.done) // Unblocks the waiter in acquireLock()
}

func (kv *KVService) releaseLock(txRecord *TxRecord) {

	for key := range txRecord.readSet {
		lockState := kv.lockTable[key]
		delete(lockState.readers, txRecord.id)
		kv.wakeNext(lockState)
	}

	for key := range txRecord.writeSet {
		lockState := kv.lockTable[key]
		lockState.writer = kvs.TXID{}
		kv.wakeNext(lockState)
	}
}

func (kv *KVService) Begin(request *kvs.BeginRequest, response *kvs.BeginResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.txTable[request.TxID]; !ok {
		txRecord := &TxRecord{
			id:       request.TxID,
			status:   kvs.TxOK,
			writeSet: make(map[string]string),
			readSet:  make(map[string]struct{}),
		}

		kv.txTable[request.TxID] = txRecord
	}

	return nil
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("Get", request)

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		return errors.New("TXN_NOT_FOUND")
	}

	// Check if the transaction has already written to this key (already holds the X-lock)
	// fmt.Println(txRecord.writeSet)
	if val, ok := txRecord.writeSet[request.Key]; ok {
		response.Value = val
		return nil
	}

	// Check if lockState for the key exists, if not create it
	lockState := kv.createLockStateIfAbsent(request.Key)

	// Try to acquire S-lock. Deny only if another transaction holds it
	kv.acquireLock(request.Key, lockState, *txRecord, "S")

	// Acquired S-lock, update read set of the transaction
	txRecord.readSet[request.Key] = struct{}{}

	// Get value from map
	if val, ok := kv.mp.Get(request.Key); ok {
		response.Value = val
	} else {
		response.Value = ""
	}

	if _, ok := kv.txTable[request.TxID]; !ok {
		return errors.New("transaction aborted")
	}

	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("Put", request)

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		return errors.New("TXN_NOT_FOUND")
	}

	// Check if the transaction has already written to this key (already holds the X-lock)
	if _, ok := txRecord.writeSet[request.Key]; ok {
		txRecord.writeSet[request.Key] = request.Value
		return nil
	}

	// Check if lockState for the key exists, if not create it
	lockState := kv.createLockStateIfAbsent(request.Key)

	// Try to acquire X-lock. Deny only if another transaction holds it
	kv.acquireLock(request.Key, lockState, *txRecord, "X")

	// Acquired X-lock, update write set of the transaction
	txRecord.writeSet[request.Key] = request.Value

	if _, ok := kv.txTable[request.TxID]; !ok {
		return errors.New("transaction aborted")
	}

	return nil

}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("Commit ", request)

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		return errors.New("TXN_NOT_FOUND")
	}

	// Do writes
	for key, val := range txRecord.writeSet {
		kv.mp.Set(key, val)
	}

	// Release locks held by the transaction
	kv.releaseLock(txRecord)

	// Remove transaction from table
	delete(kv.txTable, request.TxID)

	if request.Flag {
		kv.stats.commits++
	}

	return nil
}

func (kv *KVService) Abort(request *kvs.AbortRequest, response *kvs.AbortResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		return nil
	}

	// Release locks held by the transaction
	kv.releaseLock(txRecord)

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
	flag.Parse()

	kvs := NewKVService()
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
