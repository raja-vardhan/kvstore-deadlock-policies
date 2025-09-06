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

	cmap "github.com/orcaman/concurrent-map/v2"
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
	mp        cmap.ConcurrentMap[string, string]
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = cmap.New[string]()
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Batch(request *kvs.BatchRequest, response *kvs.BatchResponse) error {
	response.Batch = make([]kvs.RespObj, len(request.Batch))
	var gets, puts uint64 = 0, 0
	for i := 0; i < len(request.Batch); i++ {
		response.Batch[i].IsGet = request.Batch[i].IsGet
		if request.Batch[i].IsGet {
			gets++
			if value, found := kv.mp.Get(request.Batch[i].Key); found {
				response.Batch[i].Value = value
			}
		} else {
			puts++
			kv.mp.Set(request.Batch[i].Key, request.Batch[i].Value)
		}
	}
	atomic.AddUint64(&kv.stats.gets, gets)
	atomic.AddUint64(&kv.stats.puts, puts)
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
