package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Client struct {
	rpcClient *rpc.Client

	clientID     uint64                 // globally unique per client
	txID         *TxID                  // current transaction ID
	txActive     bool                   // is a transaction ongoing?
	writeSet     map[string]string      // key → value
	participants map[string]*rpc.Client // server address → rpcClient
}

type TxID struct{ Hi, Lo uint64 } // include clientID for uniqueness

func newTxID(clientID uint64) *TxID {
	return &TxID{
		Hi: clientID,
		Lo: uint64(time.Now().UnixNano()), // simple time-based randomness
	}
}

func Dial(addr string, clientID uint64) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	return &Client{rpcClient: rpcClient, clientID: clientID}
}

// func Dial(addr string) *Client {
// 	rpcClient, err := rpc.DialHTTP("tcp", addr)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	return &Client{rpcClient: rpcClient}
// }

func (c *Client) Begin() {
	c.txID = newTxID(c.clientID)
	c.txActive = true
	c.writeSet = make(map[string]string)
	c.participants = make(map[string]*rpc.Client)
}

func (c *Client) Get(key string) (string, error) {
	if !c.txActive {
		return "", fmt.Errorf("transaction not active")
	}
	if val, ok := c.writeSet[key]; ok {
		return val, nil // read-your-own-write
	}

	serverAddr := pickServerAddr(key)
	if _, ok := c.participants[serverAddr]; !ok {
		rpcClient, err := rpc.DialHTTP("tcp", serverAddr)
		if err != nil {
			return "", err
		}
		c.participants[serverAddr] = rpcClient
	}

	args := &kvs.GetRequest{
		Key:  key,
		TxID: kvs.TXID{Hi: c.txID.Hi, Lo: c.txID.Lo},
	}
	reply := &kvs.GetResponse{}
	err := c.participants[serverAddr].Call("KVService.Get", args, reply)
	if err != nil {
		return "", err
	}
	if reply.Status == kvs.TxAborted {
		c.txActive = false
		return "", fmt.Errorf("transaction aborted")
	}
	return reply.Value, nil
}

func (c *Client) Put(key, val string) error {
	if !c.txActive {
		return fmt.Errorf("transaction not active")
	}
	c.writeSet[key] = val

	serverAddr := pickServerAddr(key)
	if _, ok := c.participants[serverAddr]; !ok {
		rpcClient, err := rpc.DialHTTP("tcp", serverAddr)
		if err != nil {
			return err
		}
		c.participants[serverAddr] = rpcClient
	}

	args := &kvs.PutRequest{
		Key:   key,
		Value: val,
		TxID:  kvs.TXID{Hi: c.txID.Hi, Lo: c.txID.Lo},
	}
	reply := &kvs.PutResponse{}
	err := c.participants[serverAddr].Call("KVService.Put", args, reply)
	if err != nil {
		return err
	}
	if reply.Status == kvs.TxAborted {
		c.txActive = false
		return fmt.Errorf("transaction aborted")
	}
	return nil
}

func (c *Client) Commit() error {
	return fmt.Errorf("Commit not implemented")
}

func (c *Client) Abort() error {
	return fmt.Errorf("Abort not implemented")
}

var serverAddrs []string

// Helper to pick server address for key
func pickServerAddr(key string) string {
	// This must match the modulo rule you're using
	k := hashKey(key)
	serverIdx := k % len(serverAddrs)
	return serverAddrs[serverIdx]
}

func hashKey(key string) int {
	// Simple hash, you can replace with better one if needed
	h := 0
	for i := 0; i < len(key); i++ {
		h = int(key[i]) + 31*h
	}
	return h & 0x7fffffff
}

// func (c *Client) Begin()
// func (c *Client) Get(key string) (string, error)   // returns value or ErrTxAborted
// func (c *Client) Put(key, val string) error        // ErrTxAborted if No-Wait conflict
// func (c *Client) Commit() error                    // returns nil on success, ErrTxAborted otherwise
// func (c *Client) Abort() error

// ---------------- Broker ----------------

type BrokerConfig struct {
	BatchSize    int
	BatchTimeout time.Duration
	Value        string
}

type Broker struct {
	cfg       BrokerConfig
	client    *Client
	in        <-chan kvs.WorkloadOp
	wg        *sync.WaitGroup
	opsSent   *atomic.Uint64
	rpcErrors *atomic.Uint64
}

func (b *Broker) run(done *atomic.Bool) {
	defer b.wg.Done()

	batch := make([]kvs.ReqObj, b.cfg.BatchSize)
	count := 0

	// one reusable timer per broker
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	timerActive := false

	resetTimer := func() {
		if timerActive {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		timer.Reset(b.cfg.BatchTimeout)
		timerActive = true
	}
	stopTimer := func() {
		if timerActive {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerActive = false
		}
	}
	flush := func() {
		if count == 0 {
			return
		}
		req := kvs.BatchRequest{Batch: append([]kvs.ReqObj(nil), batch[:count]...)}
		resp := kvs.BatchResponse{}
		if err := b.client.rpcClient.Call("KVService.Batch", &req, &resp); err != nil {
			b.rpcErrors.Add(1)
		}
		b.opsSent.Add(uint64(count))
		count = 0
		stopTimer()
	}

	for !done.Load() {
		select {
		case op, ok := <-b.in:
			if !ok {
				flush()
				return
			}
			if op.IsRead {
				batch[count] = kvs.ReqObj{Key: fmt.Sprintf("%d", op.Key), IsGet: true}
			} else {
				batch[count] = kvs.ReqObj{Key: fmt.Sprintf("%d", op.Key), Value: b.cfg.Value, IsGet: false}
			}
			count++
			if count == 1 {
				resetTimer()
			}
			if count == b.cfg.BatchSize {
				flush()
			}

		case <-timer.C:
			timerActive = false
			flush()
		}
	}
	flush()
}

//----------------runClient-----------

func runClient(id int, hosts HostList, done *atomic.Bool, workload string, theta float64, batchSize int, batchTimeout int, brokersPerHost int, channelBuffer int,
	generators int, resultsCh chan<- uint64) {

	clients := make([]*Client, len(hosts))
	for i := 0; i < len(hosts); i++ {
		clients[i] = Dial(hosts[i], uint64(id))
	}

	value := strings.Repeat("x", 128)

	var wg sync.WaitGroup
	var opsSent atomic.Uint64
	var rpcErrors atomic.Uint64

	// per-host broker channels
	mq := make([][]chan kvs.WorkloadOp, len(hosts))
	brokerCounters := make([]uint64, len(hosts)) // round-robin counters

	for i := range hosts {
		mq[i] = make([]chan kvs.WorkloadOp, brokersPerHost)
		for b := 0; b < brokersPerHost; b++ {
			ch := make(chan kvs.WorkloadOp, channelBuffer)
			mq[i][b] = ch
			wg.Add(1)
			broker := &Broker{
				cfg: BrokerConfig{
					BatchSize:    batchSize,
					BatchTimeout: time.Duration(batchTimeout) * time.Millisecond,
					Value:        value,
				},
				client:    clients[i],
				in:        ch,
				wg:        &wg,
				opsSent:   &opsSent,
				rpcErrors: &rpcErrors,
			}
			go broker.run(done)
		}
	}

	opsCompleted := uint64(0)

	var genWG sync.WaitGroup

	// produce ops until done

	for g := 0; g < generators; g++ {
		genWG.Add(1)
		go func(genID int) {
			defer genWG.Done()
			wl := kvs.NewWorkload(workload, theta) // new workload generator
			for !done.Load() {
				op := wl.Next()
				hostIdx := int(op.Key) % len(hosts)

				sent := false
				start := int(atomic.AddUint64(&brokerCounters[hostIdx], 1))
				brokers := mq[hostIdx]
				L := len(brokers)

				// try each broker once, non-blocking
				for t := 0; t < L; t++ {
					b := (start + t) % L
					select {
					case brokers[b] <- op:
						sent = true
					default:
					}
					if sent {
						break
					}
				}
			}
		}(g)
	}

	// wait for generators to finish
	genWG.Wait()

	// close channels → brokers flush & exit
	for i := range mq {
		for _, ch := range mq[i] {
			close(ch)
		}
	}
	wg.Wait()

	fmt.Printf("Client %d finished ops=%d rpcErrors=%d\n", id, opsCompleted, rpcErrors.Load())
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
	batchSize := flag.Int("batchSize", 8192, "Number of ops per batch before flush")
	batchTimeout := flag.Int("batchTimeout", 10, "Max time to wait before flushing a batch (in ms)")
	brokersPerHost := flag.Int("brokersPerHost", 8, "Number of brokers per server")
	generators := flag.Int("generators", 8, "Number of workload generator goroutines per client")
	channelBuffer := flag.Int("channelBuffer", 65536, "Size of the buffer for queuing ops by the broker")
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

	start := time.Now()

	done := atomic.Bool{}
	resultsCh := make(chan uint64)

	clientId := 0
	go func(clientId int) {
		runClient(clientId, hosts, &done, *workload, *theta, *batchSize, *batchTimeout, *brokersPerHost, *channelBuffer, *generators, resultsCh)
	}(clientId)

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	opsCompleted := <-resultsCh

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
