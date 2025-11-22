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
	"github.com/rstutsman/cs6450-labs/kvs/utils"
)

var LOCK_DENIED = "LOCK_DENIED"
var TXN_NOT_FOUND = "TXN_NOT_FOUND"

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
	if a.Lo < b.Lo {
		return true
	}
	return false
}

func (kv *KVService) getLockState(key string) *LockState {
	if _, ok := kv.lockTable[key]; !ok {
		lockState := &LockState{
			readers:   make(map[kvs.TXID]struct{}),
			waitQueue: priorityqueue.NewWith(utils.TXIDComparator),
		}
		kv.lockTable[key] = lockState
	}
	return kv.lockTable[key]
}

func (kv *KVService) getTxnRecord(txID kvs.TXID) *TxRecord {
	if _, ok := kv.txTable[txID]; !ok {
		txRecord := &TxRecord{
			id:       txID,
			status:   kvs.TxOK,
			writeSet: make(map[string]string),
			readSet:  make(map[string]struct{}),
		}
		kv.txTable[txID] = txRecord
	}
	return kv.txTable[txID]
}

func canAcquire(lockState *LockState, txID kvs.TXID, mode string) bool {
	if mode == "S" {
		if lockState.writer != (kvs.TXID{}) {
			// Deny if another txn is writing to it
			if lockState.writer != txID {
				return false
			}
		} else {
			// Deny if there is no active writer and older txns are waiting
			w, ok := lockState.waitQueue.Peek()
			if ok && isOlder(w.(kvs.Waiter).TxID, txID) {
				return false
			}
		}
		lockState.readers[txID] = struct{}{}
	} else {
		if lockState.writer != (kvs.TXID{}) {
			// Deny if another txn is writing to it
			if lockState.writer != txID {
				return false
			}
		} else {
			// Deny if there is no active writer and other txns are reading
			for reader, _ := range lockState.readers {
				if reader != txID {
					return false
				}
			}
		}
		lockState.writer = txID
	}
	return true
}

func (kv *KVService) acquireLock(lockState *LockState, txID kvs.TXID, mode string) error {
	for {
		// kv mutex acquired from prev iteration or prev func call

		// Try to acquire the S/X lock
		if canAcquire(lockState, txID, mode) {
			break
		}

		// Decide to abort or wait
		if mode == "S" {
			if lockState.writer != (kvs.TXID{}) && isOlder(lockState.writer, txID) {
				return errors.New(LOCK_DENIED)
			}
			if !lockState.waitQueue.Empty() {
				return errors.New(LOCK_DENIED)
			}
		} else {
			if lockState.writer != (kvs.TXID{}) && isOlder(lockState.writer, txID) {
				return errors.New(LOCK_DENIED)
			}
			for reader, _ := range lockState.readers {
				if isOlder(reader, txID) {
					return errors.New(LOCK_DENIED)
				}
			}
		}

		// Wait for a signal
		w := kvs.Waiter{TxID: txID, Mode: mode, Done: make(chan struct{})}
		lockState.waitQueue.Enqueue(w)
		// Release mutex
		kv.mu.Unlock()

		<-w.Done

		// Acquire mutex
		kv.mu.Lock()
	}
	return nil
}

func (kv *KVService) releaseLock(lockState *LockState, txID kvs.TXID) {
	delete(lockState.readers, txID)
	if lockState.writer == txID {
		lockState.writer = kvs.TXID{}
	}
	// Ensure that top txn can progress
	if w, ok := lockState.waitQueue.Dequeue(); ok {
		close(w.(kvs.Waiter).Done)
	}
}

// func (kv *KVService) begin(txID kvs.TXID) error {

// 	txRecord := &TxRecord{
// 		id:       txID,
// 		status:   kvs.TxOK,
// 		writeSet: make(map[string]string),
// 		readSet:  make(map[string]struct{}),
// 	}

// 	kv.txTable[txID] = txRecord

// 	// fmt.Println(txRecord)

// 	return nil
// }

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lockState := kv.getLockState(request.Key)

	err := kv.acquireLock(lockState, request.TxID, "S")
	if err != nil {
		return err
	}

	txRecord := kv.getTxnRecord(request.TxID)
	txRecord.readSet[request.Key] = struct{}{}

	if val, ok := kv.mp.Get(request.Key); ok {
		response.Value = val
	} else {
		response.Value = ""
	}
	return nil
}

// func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	// fmt.Println(request)

// 	// Check if the transaction record exists
// 	txRecord := kv.getTxnRecord(request.TxID)

// 	// Check if the transaction has already written to this key (already holds the X-lock)
// 	// fmt.Println(txRecord.writeSet)
// 	if val, ok := txRecord.writeSet[request.Key]; ok {
// 		response.Value = val
// 		return nil
// 	}

// 	// Check if lockState for the key exists, if not create it
// 	lockState := kv.getLockState(request.Key)

// 	// Try to acquire S-lock. Deny only if another transaction holds it
// 	if lockState.writer != (kvs.TXID{}) && lockState.writer != request.TxID {
// 		return errors.New(LOCK_DENIED)
// 	}

// 	// Update readers of the key and the read set of the transaction
// 	lockState.readers[request.TxID] = struct{}{}
// 	txRecord.readSet[request.Key] = struct{}{}

// 	// Get value from map
// 	if val, ok := kv.mp.Get(request.Key); ok {
// 		response.Value = val
// 	} else {
// 		response.Value = ""
// 	}
// 	return nil
// }

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lockState := kv.getLockState(request.Key)

	err := kv.acquireLock(lockState, request.TxID, "X")
	if err != nil {
		return err
	}

	txRecord := kv.getTxnRecord(request.TxID)
	txRecord.writeSet[request.Key] = request.Value
	return nil
}

// func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	// fmt.Println(request)

// 	// Check if the transaction record exists
// 	txRecord := kv.getTxnRecord(request.TxID)

// 	// Check if the transaction has already written to this key (already holds the X-lock)
// 	txRecord = kv.txTable[request.TxID]
// 	if _, ok := txRecord.writeSet[request.Key]; ok {
// 		txRecord.writeSet[request.Key] = request.Value
// 		return nil
// 	}

// 	// Check if lockState for the key exists, if not create it
// 	lockState := kv.getLockState(request.Key)

// 	// Try to acquire X-lock. Return LOCK_DENIED if failed
// 	if lockState.writer != (kvs.TXID{}) {
// 		if lockState.writer != request.TxID {
// 			return errors.New(LOCK_DENIED)
// 		}
// 	} else {
// 		for key, _ := range lockState.readers {
// 			if key != request.TxID {
// 				return errors.New(LOCK_DENIED)
// 			}
// 		}
// 	}

// 	// Can safely grant lock
// 	lockState.writer = request.TxID
// 	txRecord.writeSet[request.Key] = request.Value
// 	return nil
// }

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// fmt.Println("Commit ", request)

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		return errors.New("TXN_NOT_FOUND")
	}

	// Do writes
	for key, val := range txRecord.writeSet {
		kv.mp.Set(key, val)
	}

	// Remove TxID from LockState
	for key, _ := range txRecord.writeSet {
		kv.releaseLock(kv.lockTable[key], request.TxID)
	}
	for key := range txRecord.readSet {
		kv.releaseLock(kv.lockTable[key], request.TxID)
	}

	// Remove txID from record
	delete(kv.txTable, request.TxID)

	if request.Flag {
		kv.stats.commits++
	}

	return nil
}

func (kv *KVService) Abort(request *kvs.AbortRequest, response *kvs.AbortResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// fmt.Println("Abort ", request)

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		return errors.New("TXN_NOT_FOUND")
	}

	// Remove TxID from LockState
	for key, _ := range txRecord.writeSet {
		kv.releaseLock(kv.lockTable[key], request.TxID)
	}
	for key := range txRecord.readSet {
		kv.releaseLock(kv.lockTable[key], request.TxID)
	}

	// Remove txID from record
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

	// pq.NewWith()

	http.Serve(l, nil)
}
