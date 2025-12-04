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
	TxID kvs.TXID
	Mode string
	Done chan struct{}
}

func TXIDComparator(a, b interface{}) int {
	tx1 := a.(*Waiter).TxID
	tx2 := b.(*Waiter).TxID

	if tx1.Lo < tx2.Lo {
		return 1
	}

	if tx1.Lo > tx2.Lo {
		return -1
	}

	if tx1.Hi < tx2.Hi {
		return -1
	}

	if tx1.Hi > tx2.Hi {
		return 1
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
	readers      map[kvs.TXID]struct{} // S holders
	writer       kvs.TXID              // X holder
	waitQueue    *priorityqueue.Queue  // Waiting transactions
	upgradeQueue *priorityqueue.Queue  // Txns waiting to upgrade from read to write
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
			readers:      make(map[kvs.TXID]struct{}),
			waitQueue:    priorityqueue.NewWith(TXIDComparator),
			upgradeQueue: priorityqueue.NewWith(TXIDComparator),
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
				// fmt.Println(txID, "REASON 1", lockState.writer)
				return false
			}
		} else {
			// Deny if there is no active writer and older txns are waiting
			w, ok := lockState.upgradeQueue.Peek()
			if ok {
				// fmt.Println(txID, "REASON 2.1", lockState.upgradeQueue)
				return false
			}
			w, ok = lockState.waitQueue.Peek()
			if ok && isOlder(w.(*Waiter).TxID, txID) && len(lockState.readers) > 0 {
				// fmt.Println(txID, "REASON 2.2", lockState.waitQueue)
				return false
			}
		}
		lockState.readers[txID] = struct{}{}
	} else {
		if lockState.writer != (kvs.TXID{}) {
			// Deny if another txn is writing to it
			if lockState.writer != txID {
				// fmt.Println(txID, "REASON 3", lockState.writer)
				return false
			}
		} else {
			// Deny if there is no active writer and other txns are reading
			for reader, _ := range lockState.readers {
				if reader != txID {
					// fmt.Println(txID, "REASON 4", lockState.readers)
					return false
				}
			}
		}
		lockState.writer = txID
	}
	return true
}

func (kv *KVService) acquireLock(lockState *LockState, txID kvs.TXID, mode string, key string) error {
	for {
		// kv mutex acquired from prev iteration or prev func call

		// Try to acquire the S/X lock
		if canAcquire(lockState, txID, mode) {
			break
		}
		// fmt.Println(txID, "CANT ACQUIRE", key)

		// Decide to abort or wait
		upgradeLock := false
		if mode == "S" {
			if lockState.writer != (kvs.TXID{}) && isOlder(lockState.writer, txID) {
				// fmt.Println(txID, "ABORT 1", lockState.writer)
				return errors.New(LOCK_DENIED)
			}
			if !lockState.upgradeQueue.Empty() {
				// fmt.Println(txID, "ABORT 2.1")
				return errors.New(LOCK_DENIED)
			}
			if !lockState.waitQueue.Empty() {
				// fmt.Println(txID, "ABORT 2.2")
				return errors.New(LOCK_DENIED)
			}
		} else {
			if lockState.writer != (kvs.TXID{}) && isOlder(lockState.writer, txID) {
				// fmt.Println(txID, "ABORT 3", lockState.writer)
				return errors.New(LOCK_DENIED)
			}
			for reader, _ := range lockState.readers {
				if reader == txID {
					upgradeLock = true
				}
				if isOlder(reader, txID) {
					// fmt.Println(txID, "ABORT 4", lockState.readers)
					return errors.New(LOCK_DENIED)
				}
			}
		}

		// Wait for a signal
		w := Waiter{TxID: txID, Mode: mode, Done: make(chan struct{})}
		if upgradeLock {
			if top, ok := lockState.upgradeQueue.Peek(); !ok {
				lockState.upgradeQueue.Enqueue(&w)
			} else {
				if isOlder(top.(*Waiter).TxID, txID) {
					// fmt.Println(txID, "ABORT 5", lockState.upgradeQueue)
					return errors.New(LOCK_DENIED)
				} else {
					// Dequeue and abort top. add w instead
					top, _ = lockState.upgradeQueue.Dequeue()
					lockState.upgradeQueue.Enqueue(&w)
					close(top.(*Waiter).Done)
				}
			}
		} else {
			if top, ok := lockState.waitQueue.Peek(); !ok {
				lockState.waitQueue.Enqueue(&w)
			} else {
				if isOlder(top.(*Waiter).TxID, txID) {
					// fmt.Println(txID, "ABORT 6", lockState.waitQueue)
					return errors.New(LOCK_DENIED)
				} else {
					// Dequeue and abort top. add w instead
					top, _ = lockState.waitQueue.Dequeue()
					lockState.waitQueue.Enqueue(&w)
					close(top.(*Waiter).Done)
				}
			}
		}
		// Release mutex
		kv.mu.Unlock()

		// fmt.Println(txID, "WAITING FOR", key)
		<-w.Done
		// fmt.Println(txID, "SIGNAL RECEIVED", key)

		// Acquire mutex
		kv.mu.Lock()
	}
	return nil
}

func (kv *KVService) releaseLock(lockState *LockState, txID kvs.TXID, key string) {
	delete(lockState.readers, txID)
	if lockState.writer == txID {
		lockState.writer = kvs.TXID{}
	}
	// Ensure that top txn can progress
	if w, ok := lockState.upgradeQueue.Dequeue(); ok {
		close(w.(*Waiter).Done)
		// fmt.Println(txID, "UpgKey:", key, "Queue:", lockState.upgradeQueue.Size())
	} else if w, ok := lockState.waitQueue.Dequeue(); ok {
		close(w.(*Waiter).Done)
		// fmt.Println(txID, "WaitKey:", key, "Queue:", lockState.waitQueue.Size())
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

	// fmt.Println(request.TxID, "GETTING", request.Key)

	lockState := kv.getLockState(request.Key)
	txRecord := kv.getTxnRecord(request.TxID)

	err := kv.acquireLock(lockState, request.TxID, "S", request.Key)
	if err != nil {
		return err
	}

	// fmt.Println(request.TxID, "READING", request.Key)
	txRecord.readSet[request.Key] = struct{}{}

	if val, ok := kv.mp.Get(request.Key); ok {
		response.Value = val
	} else {
		response.Value = ""
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// fmt.Println(request.TxID, "PUTTING", request.Key)

	lockState := kv.getLockState(request.Key)
	txRecord := kv.getTxnRecord(request.TxID)

	err := kv.acquireLock(lockState, request.TxID, "X", request.Key)
	if err != nil {
		return err
	}

	// fmt.Println(request.TxID, "WRITING", request.Key)
	txRecord.writeSet[request.Key] = request.Value
	return nil
}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// fmt.Println(request.TxID, "RELEASE1")

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
		kv.releaseLock(kv.lockTable[key], request.TxID, key)
	}
	for key := range txRecord.readSet {
		kv.releaseLock(kv.lockTable[key], request.TxID, key)
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

	// fmt.Println(request.TxID, "RELEASE2")

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		return errors.New("TXN_NOT_FOUND")
	}

	// Remove TxID from LockState
	for key, _ := range txRecord.writeSet {
		kv.releaseLock(kv.lockTable[key], request.TxID, key)
	}
	for key := range txRecord.readSet {
		kv.releaseLock(kv.lockTable[key], request.TxID, key)
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
	fmt.Println("waitdie")
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
