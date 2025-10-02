package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/rpc"
	"sync/atomic"
	"time"
	"errors"

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/rstutsman/cs6450-labs/kvs"
)

LOCK_DENIED := "LOCK_DENIED"
TXN_NOT_FOUND := "TXN_NOT_FOUND"

type TxRecord struct {
	id TxID
	status TxStatus
	writeSet map[string]string   // staged writes
	readSet map[string]struct{}  // optional
}

type LockState struct {
	readers map[TxID]struct{}    // S holders
	writer TxID                 // X holder
}

type Stats struct {
	commits uint64
	aborts uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.commits = s.commits - prev.commits
	r.commits = s.aborts - prev.aborts
	return r
}

type KVService struct {
	mu	sync.Mutex
	mp        cmap.ConcurrentMap[string, string]
	locks	map[string]*LockState
	txTable	map[TxID]*TxRecord
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = cmap.New[string]()
	kvs.locks = make(map[string]*LockState)
	kvs.txTable = make(map[TxID]*TxRecord)
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) begin(txID TxID) error {
	txRecord := &TxRecord {
			id: request.txID, 
			status: Active,
			writeSet: make(map[string]string),
			readSet: make(map[string]struct{})
		}
	
	kv.txTable[txID] = txRecord

	return nil
}

func (kv *KVService) TxnGet(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		begin(request.TxID)
	}

	// Check if the transaction has already written to this key (already holds the X-lock)
	if val, ok := txRecord.writeSet[request.Key] ; ok {
		response.Value = val
		return nil
	}

	// Check if lockState for the key exists, if not create it
	lockState, ok := kv.locks[request.Key]
	if !ok {
		lockState = &LockState{
			readers: make(map[TxID]struct{})
		}
		kv.locks[request.Key] = lockState
	}

	// Try to acquire S-lock. Deny only if another transaction holds it
	if lockState.writer != TxID{} && lockState.writer != request.TxID {
		return errors.New(LOCK_DENIED)
	}

	// Update readers of the key and the read set of the transaction
	lockState.readers[request.TxID] = struct{}{}
	txRecord.readSet[request.Key] = struct{}{}

	// Get value from map
	if val, ok := kv.mp.Get(request.Key) ; ok {
		response.Value = val
	} else {
		response.Value = ""
	}
	return nil
}

func (kv *KVService) TxnPut(request *kvs.PutRequest, response *kvs.PutResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		begin(request.TxID)
	}

	// Check if the transaction has already written to this key (already holds the X-lock)
	if val, ok := txRecord.writeSet[request.Key] ; ok {
		txRecord.writeSet[request.Key] = request.Value
		return nil
	}

	// Check if lockState for the key exists, if not create it
	lockState, ok := kv.locks[request.Key]
	if !ok {
		lockState = &LockState{
			readers: make(map[TxID]struct{})
		}
		kv.locks[request.Key] = lockState
	}
	
	// Try to acquire X-lock. Return LOCK_DENIED if failed
	if lockState.writer != TxID{} {
		if lockState.writer != request.TxID {
			return errors.New(LOCK_DENIED)
		}
	} else {
		for item := range kv.mp.IterBuffered() {
			if (item.Key != request.TxID) {
				return errors.New(LOCK_DENIED)
			}
		}
	}

	// Can safely grant lock
	lockState.writer = request.TxID
	txRecord.writeSet[request.Key] = request.Value
	return nil

}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		return errors.New("TXN_NOT_FOUND")
	}

	// Do writes
	for key, val := range txRecord.writeSet {
		kv.mp.Set(k, v)
	}

	// Remove TxID from LockState
	for key, val := txRecord.writeSet {
		kv.locks[key].writer = TxID{}
	}
	for key := txRecord.readSet {
		delete(kv.locks[key].readers, request.TxID)
	}

	// Remove txID from record
	delete(kv.txTable, request.TxID)

	if request.flag {
		kv.stats.commits++
	}

	return nil
}

func (kv *KVService) Abort(request *kvs.AbortRequest, response *kvs.AbortResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if the transaction record exists
	txRecord, ok := kv.txTable[request.TxID]
	if !ok {
		return errors.New("TXN_NOT_FOUND")
	}

	// Remove TxID from LockState
	for key, val := txRecord.writeSet {
		kv.locks[key].writer = TxID{}
	}
	for key := txRecord.readSet {
		delete(kv.locks[key].readers, request.TxID)
	}

	// Remove txID from record
	delete(kv.txTable, request.TxID)

	if request.flag {
		kv.stats.aborts++
	}

	return nil
}

func (kv *KVService) printStats() {

	stats := Stats{
		gets: atomic.LoadUint64(&kv.stats.gets),
		puts: atomic.LoadUint64(&kv.stats.puts),
	}

	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS)
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
