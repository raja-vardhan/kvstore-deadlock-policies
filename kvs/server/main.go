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

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/rstutsman/cs6450-labs/kvs"
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
	readers map[kvs.TXID]struct{} // S holders
	writer  kvs.TXID              // X holder
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
	locks     map[string]*LockState
	txTable   map[kvs.TXID]*TxRecord
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvService := &KVService{}
	kvService.mp = cmap.New[string]()
	kvService.locks = make(map[string]*LockState)
	kvService.txTable = make(map[kvs.TXID]*TxRecord)
	kvService.lastPrint = time.Now()
	return kvService
}

func (kv *KVService) begin(txID kvs.TXID) error {
	txRecord := &TxRecord{
		id:       txID,
		status:   kvs.TxOK,
		writeSet: make(map[string]string),
		readSet:  make(map[string]struct{}),
	}

	kv.txTable[txID] = txRecord

	// fmt.Println(txRecord)

	return nil
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// fmt.Println(request)

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		kv.begin(request.TxID)
	}

	// Check if the transaction has already written to this key (already holds the X-lock)
	txRecord = kv.txTable[request.TxID]
	// fmt.Println(txRecord.writeSet)
	if val, ok := txRecord.writeSet[request.Key]; ok {
		response.Value = val
		return nil
	}

	// Check if lockState for the key exists, if not create it
	lockState, ok := kv.locks[request.Key]
	if !ok {
		lockState = &LockState{
			readers: make(map[kvs.TXID]struct{}),
		}
		kv.locks[request.Key] = lockState
	}

	// Try to acquire S-lock. Deny only if another transaction holds it
	if lockState.writer != (kvs.TXID{}) && lockState.writer != request.TxID {
		return errors.New(LOCK_DENIED)
	}

	// Update readers of the key and the read set of the transaction
	lockState.readers[request.TxID] = struct{}{}
	txRecord.readSet[request.Key] = struct{}{}

	// Get value from map
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

	// fmt.Println(request)

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		kv.begin(request.TxID)
	}

	// Check if the transaction has already written to this key (already holds the X-lock)
	txRecord = kv.txTable[request.TxID]
	if _, ok := txRecord.writeSet[request.Key]; ok {
		txRecord.writeSet[request.Key] = request.Value
		return nil
	}

	// Check if lockState for the key exists, if not create it
	lockState, ok := kv.locks[request.Key]
	if !ok {
		lockState = &LockState{
			readers: make(map[kvs.TXID]struct{}),
		}
		kv.locks[request.Key] = lockState
	}

	// Try to acquire X-lock. Return LOCK_DENIED if failed
	if lockState.writer != (kvs.TXID{}) {
		if lockState.writer != request.TxID {
			return errors.New(LOCK_DENIED)
		}
	} else {
		for key, _ := range lockState.readers {
			if key != request.TxID {
				return errors.New(LOCK_DENIED)
			}
		}
	}

	// Can safely grant lock
	lockState.writer = request.TxID
	txRecord.writeSet[request.Key] = request.Value
	return nil

}

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
		kv.locks[key].writer = kvs.TXID{}
	}
	for key := range txRecord.readSet {
		delete(kv.locks[key].readers, request.TxID)
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
		kv.locks[key].writer = kvs.TXID{}
	}
	for key := range txRecord.readSet {
		delete(kv.locks[key].readers, request.TxID)
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

	http.Serve(l, nil)
}
