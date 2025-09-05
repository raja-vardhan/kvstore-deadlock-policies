package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Client struct {
	rpcClient *rpc.Client
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient}
}

func (client *Client) Get(key string) string {
	request := kvs.GetRequest{
		Key: key,
	}
	response := kvs.GetResponse{}
	// start := time.Now().UnixMicro()
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	// end := time.Now().UnixMicro()
	// fmt.Println("Get Time", end-start)
	if err != nil {
		log.Fatal(err)
	}

	return response.Value
}

func (client *Client) Put(key string, value string) {
	request := kvs.PutRequest{
		Key:   key,
		Value: value,
	}
	response := kvs.PutResponse{}
	// start := time.Now().UnixMicro()
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	// end := time.Now().UnixMicro()
	// fmt.Println("Put Time", end-start)
	if err != nil {
		log.Fatal(err)
	}
}

func (client *Client) Batch(request kvs.BatchRequest, response kvs.BatchResponse, count *int, op kvs.WorkloadOp, value ...string) bool {
	key := fmt.Sprintf("%d", op.Key)
	if op.IsRead {
		request.Batch[*count] = kvs.ReqObj{Key: key, IsGet: true}
	} else {
		request.Batch[*count] = kvs.ReqObj{Key: key, Value: value[0], IsGet: false}
	}
	*count++

	if *count == len(request.Batch) {
		*count = 0
		err := client.rpcClient.Call("KVService.Batch", &request, &response)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(len(request.Batch), len(response.Batch))
		return true
	}
	return false
}

func (client *Client) MessageBroker(mq chan *kvs.WorkloadOp) {
	batchSize := 100_000
	count := 0
	reqBatch := kvs.BatchRequest{Batch: make([]kvs.ReqObj, batchSize)}
	var respBatch kvs.BatchResponse
	prev := time.Now().UnixMilli()
	for {
		op := <-mq
		switch count {
		case 0:
			now := time.Now().UnixMilli()
			fmt.Println("Time to response", now-prev)
			prev = now
		case batchSize - 1:
			now := time.Now().UnixMilli()
			fmt.Println("Time to fill", now-prev)
			prev = now
		}
		// key := fmt.Sprintf("%d", op.Key)
		value := strings.Repeat("x", 128)
		if op.IsRead {
			// go func() {
			// client.Get(key)
			client.Batch(reqBatch, respBatch, &count, *op)
			// }()
		} else {
			// go func() {
			// client.Put(key, value)
			client.Batch(reqBatch, respBatch, &count, *op, value)
			// }()
		}
	}
}

func runClient(id int, hosts HostList, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := make([]*Client, len(hosts))
	for i := 0; i < len(client); i++ {
		client[i] = Dial(hosts[i])
	}

	buffer := 10_000_000 // Vary buffer
	mq := make([]chan *kvs.WorkloadOp, len(client))
	for i := 0; i < len(mq); i++ {
		mq[i] = make(chan *kvs.WorkloadOp, buffer)
		// TODO: Rework to come up with a scheduling strategy here so that when 1 go routine queue to a server is waiting. we start filling a different batch request to the same server
		go client[i].MessageBroker(mq[i])
		go client[i].MessageBroker(mq[i])
	}

	// value := strings.Repeat("x", 128)
	const batchSize = 1024

	opsCompleted := uint64(0)

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			// key := fmt.Sprintf("%d", op.Key)
			// start := time.Now().UnixMicro()
			// if op.IsRead {
			// 	// go func() {
			// 	client[int(op.Key)%len(client)].Get(key)
			// 	// }()
			// } else {
			// 	// go func() {
			// 	client[int(op.Key)%len(client)].Put(key, value)
			// 	// }()
			// }
			// end := time.Now().UnixMicro()
			// fmt.Println("Op Time", end-start)
			mq[int(op.Key)%len(mq)] <- &op
			opsCompleted++
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)

	resultsCh <- opsCompleted
}

type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	hosts := HostList{}

	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf(
		"hosts %v\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n",
		hosts, *theta, *workload, *secs,
	)

	start := time.Now()

	done := atomic.Bool{}
	resultsCh := make(chan uint64)

	// host := hosts[0]
	clientId := 0
	go func(clientId int) {
		workload := kvs.NewWorkload(*workload, *theta)
		runClient(clientId, hosts, &done, workload, resultsCh)
	}(clientId)

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	opsCompleted := <-resultsCh

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
