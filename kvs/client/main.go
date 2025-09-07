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
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient}
}

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

func runClient(id int, hosts HostList, done *atomic.Bool, workload string, theta float64, resultsCh chan<- uint64) {
	clients := make([]*Client, len(hosts))
	for i := 0; i < len(hosts); i++ {
		clients[i] = Dial(hosts[i])
	}

	// config
	const batchSize = 8192
	const batchTimeout = 10 * time.Millisecond
	const brokersPerHost = 8
	const channelBuf = 65536
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
			ch := make(chan kvs.WorkloadOp, channelBuf)
			mq[i][b] = ch
			wg.Add(1)
			broker := &Broker{
				cfg: BrokerConfig{
					BatchSize:    batchSize,
					BatchTimeout: batchTimeout,
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

	for g := 0; g < 8; g++ {
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

				// try each broker once, non-blocking (fast path)
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

				// // if all queues are full, block on the round-robin broker (backpressure)
				// if !sent {
				// 		b := start % L
				// 		brokers[b] <- op
				// }
				// atomic.AddUint64(&opsCompleted, 1)

				// // round-robin broker choice
				// next := atomic.AddUint64(&brokerCounters[hostIdx], 1)
				// brokerIdx := int(next) % len(mq[hostIdx])

				// select {
				// case mq[hostIdx][brokerIdx] <- op:
				// 	atomic.AddUint64(&opsCompleted, 1)
				// default:
				// 	// drop if channel full
				// }
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

	clientId := 0
	go func(clientId int) {
		runClient(clientId, hosts, &done, *workload, *theta, resultsCh)
	}(clientId)

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	opsCompleted := <-resultsCh

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
