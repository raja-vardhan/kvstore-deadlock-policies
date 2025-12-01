package main

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
	"github.com/rstutsman/cs6450-labs/kvs/utils"
)

type Worker struct {
	rpcClients []*rpc.Client // RPC clients to all the servers
	workerID   uint64        // globally unique per worker
	txID       *kvs.TXID     // current transaction ID
	txActive   bool          // is a transaction ongoing?
	policy string			//'nowait' ,'waitdie', 'woundwait'
}

func newTxID(clientID uint64) kvs.TXID {

	var rnd uint64
	binary.Read(rand.Reader, binary.BigEndian, &rnd)

	return kvs.TXID{
		ID: uint32(clientID),
		Hi: uint64(time.Now().UnixNano()),
		Lo: rnd,
	}
}

func Dial(addr string) *rpc.Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	return rpcClient
}

var serverAddrs []string

// Helper to pick server address for key
func getServerIdx(key string) int {
	k := hashKey(key)
	serverIdx := k % len(serverAddrs)
	return serverIdx
}

func hashKey(key string) int {
	// Simple hash, you can replace with better one if needed
	// h := 0
	// for i := 0; i < len(key); i++ {
	// 	h = int(key[i]) + 31*h
	// }
	// return h & 0x7fffffff
	h, _ := strconv.Atoi(key)
	return h
}

func (c *Worker) Begin(txID *kvs.TXID) error {

	c.txID = txID
	c.txActive = true

	args := kvs.BeginRequest{
		TxID: *txID,
	}
	reply := &kvs.BeginResponse{}

	for serverIdx := range len(serverAddrs) {
		err := c.rpcClients[serverIdx].Call("KVService.Begin", args, reply)

		if err != nil {
			return errors.New("cannot start transaction")
		}
	}

	return nil
}

func (c *Worker) Get(key string) (string, error) {
	if !c.txActive {
		return "", fmt.Errorf("transaction not active")
	}

	serverIdx := getServerIdx(key)

	args := &kvs.GetRequest{
		Key:  key,
		TxID: *c.txID,
	}
	reply := &kvs.GetResponse{}

	err := c.rpcClients[serverIdx].Call("KVService.Get", args, reply)
	if err != nil {
		return "", err
	}

	if reply.Status == kvs.TxAborted {
		return "", fmt.Errorf("transaction aborted")
	}
	return reply.Value, nil
}

func (c *Worker) Put(key, val string) error {
	if !c.txActive {
		return fmt.Errorf("transaction not active")
	}

	serverIdx := getServerIdx(key)

	args := &kvs.PutRequest{
		Key:   key,
		Value: val,
		TxID:  *c.txID,
	}
	reply := &kvs.PutResponse{}

	err := c.rpcClients[serverIdx].Call("KVService.Put", args, reply)
	if err != nil {
		return err
	}
	if reply.Status == kvs.TxAborted {
		return fmt.Errorf("transaction aborted")
	}

	return nil
}

func (c *Worker) Commit() error {
	if !c.txActive {
		return fmt.Errorf("transaction not active")
	}

	// Prepare phase
	/*
	prepareArgs := &kvs.PrepareRequest{TxID: *c.txID}
	prepareReply := &kvs.PrepareResponse{}

	for idx := range len(serverAddrs) {
		err := c.rpcClients[idx].Call("KVService.Prepare", prepareArgs, prepareReply)
		if err != nil || prepareReply.Status == kvs.TxAborted {
			return fmt.Errorf("prepare failed")
		}
	}
	*/

	if c.policy == "woundwait"{
		prepareArgs := &kvs.PrepareRequest{TxID: *c.txID}
		prepareReply := &kvs.PrepareResponse{}

		for idx := range len(serverAddrs){
			err := c.rpcClients[idx].Call("KVService.Prepare", prepareArgs, prepareReply)
			if err != nil || prepareReply.Status == kvs.TxAborted{
				return fmt.Errorf("prepare failed")
			}
		}
	}

	// Commit phase
	commitArgs := &kvs.CommitRequest{TxID: *c.txID}
	commitReply := &kvs.CommitResponse{}

	for idx := range len(serverAddrs) {
		if idx == 0 {
			commitArgs.Flag = true
		} else {
			commitArgs.Flag = false
		}

		err := c.rpcClients[idx].Call("KVService.Commit", commitArgs, commitReply)
		if err != nil {
			return err
		}
	}

	c.txActive = false
	c.txID = nil
	return nil
}

func (c *Worker) Abort() error {
	if !c.txActive {
		return fmt.Errorf("transaction not active")
	}

	args := &kvs.AbortRequest{TxID: kvs.TXID(*c.txID)}
	reply := &kvs.AbortResponse{}

	for idx := range len(serverAddrs) {
		if idx == 0 {
			args.Flag = true
		} else {
			args.Flag = false
		}

		err := c.rpcClients[idx].Call("KVService.Abort", args, reply)
		if err != nil {
			return err
		}
	}

	c.txActive = false
	c.txID = nil
	return nil
}

//----------------workloads-----------

func runClient(id int, hosts HostList, done *atomic.Bool, workload string, theta float64, opsPerTx int, policy string) {

	rpcClients := make([]*rpc.Client, len(hosts))
	for i, host := range hosts {
		rpcClients[i] = Dial(host)
	}

	// Initialize work queue
	transactionQueue := make(chan *kvs.Transaction, 10000)

	// Initialize producer
	wl := kvs.NewWorkload(workload, theta)
	go func() {
		for !done.Load() {
			transactionQueue <- utils.GenerateRandomTransaction(wl, opsPerTx)
		}
	}()

	// Initialize workers (pick up transaction from work queue, execute and abort if required)
	NUM_OF_WORKERS := 10
	for i := 0; i < NUM_OF_WORKERS; i++ {
		go func() {
			//worker := Worker{rpcClients: rpcClients, workerID: uint64(i)}
			worker := Worker{rpcClients: rpcClients, workerID: uint64(i), policy: policy}

			for !done.Load() {

				txn := <-transactionQueue

				txn.TxID = newTxID(worker.workerID)

				txnDone := false

				// Execute transaction
				for !txnDone {
					for _, op := range txn.Ops {

						var err error

						switch op.Type {
						case kvs.OpBegin:
							err = worker.Begin(&txn.TxID)
						case kvs.OpGet:
							_, err = worker.Get(op.Key)
						case kvs.OpPut:
							err = worker.Put(op.Key, op.Value)
						case kvs.OpCommit:
							err = worker.Commit()
						default:
						}

						if err != nil {
							worker.Abort()
							break
						}

						if op.Type == kvs.OpCommit && err == nil {
							txnDone = true
						}

					}
				}

			}
		}()

	}

}

func serializabilityTest(id int, hosts HostList, done *atomic.Bool, numClients int, initDone *atomic.Bool) {
	// fmt.Println("Inside xfer")
	rpcClients := make([]*rpc.Client, len(hosts))
	for i, host := range hosts {
		rpcClients[i] = Dial(host)
	}
	// fmt.Println("Established connections")
	client := Worker{rpcClients: rpcClients, workerID: uint64(id)}
	checkBal := 0
	initAmt := 1000
	txnAmt := 100
	total := initAmt * numClients
	balances := make([]int, numClients)
	freq := 10
	for !done.Load() {
		abort := false
		if !initDone.Load() {
			if id != 0 {
				continue
			}

			txID := newTxID(uint64(id))
			client.txID = &txID
			client.Begin(&txID)

			for i := 0; i < numClients; i++ {
				acctId := fmt.Sprintf("%d", i)
				amt := fmt.Sprintf("%d", initAmt)
				err := client.Put(acctId, amt)
				if err != nil {
					abort = true
					break
				}
			}
			if abort {
				client.Abort()
			} else {
				err := client.Commit()
				if err == nil {
					initDone.Store(true)
					checkBal = (checkBal + 1) % freq
					fmt.Println("Initialized all accounts with 1000")
				} else {
					client.Abort()
				}
			}
		} else {
			if checkBal == 1 {

				txID := newTxID(uint64(id))
				client.txID = &txID
				client.Begin(&txID)

				sum := 0
				balances = balances[:0]
				for i := 0; i < numClients; i++ {
					acctId := fmt.Sprintf("%d", i)
					bal, err := client.Get(acctId)
					if err != nil {
						abort = true
						break
					}
					currBal, _ := strconv.Atoi(bal)
					balances = append(balances, currBal)
					sum += currBal
				}
				if abort {
					client.Abort()
				} else {
					err := client.Commit()

					if err == nil {
						checkBal = (checkBal + 1) % freq
						if sum != total {
							fmt.Println("VIOLATION!!!")
						}
						fmt.Println("Sum:", sum, balances)
					} else {
						client.Abort()
					}
				}
			} else {

				txID := newTxID(uint64(id))
				client.Begin(&txID)

				src := fmt.Sprintf("%d", id)
				dst := fmt.Sprintf("%d", (id+1)%numClients)
				srcBal, err := client.Get(src)
				if err != nil {
					client.Abort()
					continue
				}
				sBal, _ := strconv.Atoi(srcBal)
				if sBal < txnAmt {
					client.Abort()
					continue
				}
				dstBal, err := client.Get(dst)
				if err != nil {
					client.Abort()
					continue
				}
				dBal, _ := strconv.Atoi(dstBal)
				srcBal = fmt.Sprintf("%d", sBal-txnAmt)
				dstBal = fmt.Sprintf("%d", dBal+txnAmt)
				err = client.Put(dst, dstBal)
				if err != nil {
					client.Abort()
					continue
				}
				err = client.Put(src, srcBal)
				if err != nil {
					client.Abort()
					continue
				}
				err = client.Commit()
				if err == nil {
					checkBal = (checkBal + 1) % freq
				} else {
					client.Abort()
				}
			}
		}
	}
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
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C, xfer)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	opsPerTx := flag.Int("opsPerTx", 3, "Number of Get or Put operations per transaction")
	policy := flag.String("policy", "nowait", "Lock policy: nowait|waitdie|woundwait")
	// batchSize := flag.Int("batchSize", 8192, "Number of ops per batch before flush")
	// batchTimeout := flag.Int("batchTimeout", 10, "Max time to wait before flushing a batch (in ms)")
	// brokersPerHost := flag.Int("brokersPerHost", 8, "Number of brokers per server")
	// generators := flag.Int("generators", 8, "Number of workload generator goroutines per client")
	// channelBuffer := flag.Int("channelBuffer", 65536, "Size of the buffer for queuing ops by the broker")
	flag.Parse()

	serverAddrs = hosts // so pickServerAddr can use it

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

	done := atomic.Bool{}

	if *workload != "xfer" {
		clientId := os.Getpid() // TODO: Check if this is unique across different client processes. Original value was 0
		go func(clientId int) {
			runClient(clientId, hosts, &done, *workload, *theta, *opsPerTx, *policy)
		}(clientId)
	} else {
		numClients := 10
		initDone := atomic.Bool{}
		for clientId := 0; clientId < numClients; clientId++ {
			go func(clientId int) {
				serializabilityTest(clientId, hosts, &done, numClients, &initDone)
			}(clientId)
		}
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)
}