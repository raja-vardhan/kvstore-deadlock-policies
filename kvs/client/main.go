package main

import (
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
)

type Client struct {
	rpcClients   []*rpc.Client // RPC clients to all the servers
	clientID     uint64        // globally unique per client
	txID         *TxID         // current transaction ID
	txActive     bool          // is a transaction ongoing?
	participants map[int]bool  // idx -> rpcClient
}

type TxID struct{ Hi, Lo uint64 } // include clientID for uniqueness

func newTxID(clientID uint64) *TxID {
	return &TxID{
		Hi: clientID,
		Lo: uint64(time.Now().UnixNano()), // simple time-based randomness
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
	// This must match the modulo rule you're using
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

func (c *Client) Begin() {
	c.txID = newTxID(c.clientID)
	c.txActive = true
	c.participants = make(map[int]bool, len(c.rpcClients))
}

func (c *Client) Get(key string) (string, error) {
	if !c.txActive {
		return "", fmt.Errorf("transaction not active")
	}

	serverIdx := getServerIdx(key)
	if _, ok := c.participants[serverIdx]; !ok {
		c.participants[serverIdx] = true
	}

	args := &kvs.GetRequest{
		Key:  key,
		TxID: kvs.TXID{Hi: c.txID.Hi, Lo: c.txID.Lo},
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

func (c *Client) Put(key, val string) error {
	if !c.txActive {
		return fmt.Errorf("transaction not active")
	}

	serverIdx := getServerIdx(key)
	if _, ok := c.participants[serverIdx]; !ok {
		c.participants[serverIdx] = true
	}

	args := &kvs.PutRequest{
		Key:   key,
		Value: val,
		TxID:  kvs.TXID{Hi: c.txID.Hi, Lo: c.txID.Lo},
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

func (c *Client) Commit() error {
	if !c.txActive {
		return fmt.Errorf("transaction not active")
	}

	args := &kvs.CommitRequest{TxID: kvs.TXID(*c.txID)}
	reply := &kvs.CommitResponse{}
	count := 0
	for idx, _ := range c.participants {
		if count == 0 {
			args.Flag = true
			count++
		}
		err := c.rpcClients[idx].Call("KVService.Commit", args, reply)
		if err != nil {
			return err
		}
	}
	c.txActive = false
	c.txID = nil
	c.participants = nil
	return nil
}

func (c *Client) Abort() error {
	if !c.txActive {
		return fmt.Errorf("transaction not active")
	}

	args := &kvs.AbortRequest{TxID: kvs.TXID(*c.txID)}
	reply := &kvs.AbortResponse{}
	count := 0
	for idx, _ := range c.participants {
		if count == 0 {
			args.Flag = true
			count++
		}
		err := c.rpcClients[idx].Call("KVService.Abort", args, reply)
		if err != nil {
			return err
		}
	}
	return nil
}

//----------------workloads-----------

func runClient(id int, hosts HostList, done *atomic.Bool, workload string, theta float64, opsPerTx int) {
	rpcClients := make([]*rpc.Client, len(hosts))
	for i, host := range hosts {
		rpcClients[i] = Dial(host)
	}
	client := Client{rpcClients: rpcClients, clientID: uint64(id)}
	wl := kvs.NewWorkload(workload, theta)
	value := strings.Repeat("x", 128)
	txOps := make([]kvs.WorkloadOp, opsPerTx)
	for !done.Load() {
		abort := false
		if !client.txActive { // If true retry the previous transaction
			client.Begin()
			txOps = txOps[:0] // Empty the transaction list as we do not need to repeat the transaction
			for i := range opsPerTx {
				txOps = append(txOps, wl.Next())
				_ = i
			}
		}
		for _, op := range txOps {
			key := fmt.Sprintf("%d", op.Key)
			var err error
			if op.IsRead {
				_, err = client.Get(key)
			} else {
				err = client.Put(key, value)
			}
			if err != nil {
				fmt.Println(err)
				abort = true
				break
			}
		}
		if abort {
			client.Abort()
		} else {
			client.Commit()
		}
	}
}

func serializabilityTest(id int, hosts HostList, done *atomic.Bool, numClients int, initDone *atomic.Bool) {
	rpcClients := make([]*rpc.Client, len(hosts))
	for i, host := range hosts {
		rpcClients[i] = Dial(host)
	}
	client := Client{rpcClients: rpcClients, clientID: uint64(id)}
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
			if !client.txActive {
				client.Begin()
			}
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
				client.Commit()
				initDone.Store(true)
				checkBal = (checkBal + 1) % freq
			}
		} else {
			if checkBal == 1 {
				if !client.txActive {
					client.Begin()
				}
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
					client.Commit()
					checkBal = (checkBal + 1) % freq
					if sum != total {
						fmt.Println("VIOLATION!!!")
					}
					fmt.Println("Sum:", sum, balances)
				}
			} else {
				if !client.txActive {
					client.Begin()
				}
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
				client.Commit()
				checkBal = (checkBal + 1) % freq
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
			runClient(clientId, hosts, &done, *workload, *theta, *opsPerTx)
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
