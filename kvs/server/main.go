package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/rpc"
	"sync"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Stats struct {
	puts uint64
	gets uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = s.puts - prev.puts
	r.gets = s.gets - prev.gets
	return r
}

type KVService struct {
	sync.RWMutex
	mp        sync.Map
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	// kvs.mp = make(map[string]string)
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	// kv.RLock()
	// defer kv.RUnlock()

	kv.stats.gets++

	if value, found := kv.mp.Load(request.Key); found {
		response.Value = value.(string)
	}

	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	// kv.Lock()
	// defer kv.Unlock()

	kv.stats.puts++

	kv.mp.Store(request.Key, request.Value)

	return nil
}

func (kv *KVService) Batch(request *kvs.BatchRequest, response *kvs.BatchResponse) error {
	response.Batch = make([]kvs.RespObj, len(request.Batch))
	fmt.Println(len(request.Batch), len(response.Batch))
	for i := 0; i < len(request.Batch); i++ {
		response.Batch[i].IsGet = request.Batch[i].IsGet
		if request.Batch[i].IsGet {
			kv.stats.gets++
			if value, found := kv.mp.Load(request.Batch[i].Key); found {
				response.Batch[i].Value = value.(string)
			}
		} else {
			kv.stats.puts++
			kv.mp.Store(request.Batch[i].Key, request.Batch[i].Value)
		}
	}
	return nil
}

func (kv *KVService) printStats() {
	// kv.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	// kv.Unlock()

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
