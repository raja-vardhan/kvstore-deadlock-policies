# High-Performance Key-Value Store

## Results

### Final Throughput Numbers
In our implementation of the High-Performance Key-Value Store, we achieved a peak throughput of **19.43M ops/sec** (19435544 ops/s) using 8 CloudLab `m510` nodes and the YCSB-B workload (95% gets, 5% puts, θ = 0.99). Our system was tested using the provided workload generator, maintaining full workload integrity and linearizability under normal operation.

### Hardware Utilization
- **CPU**: ~65% usage per core on server nodes during peak load
- **Memory**: ~1 GB per node
- **Network**: Sustained ~9.5 Gbps throughput over 10 Gbps interfaces

### Scaling Characteristics
At 4 nodes(2 client, 2 server) we achieve 7.2M ops/sec. When we scale the cluster to 8 nodes, we achieve 19M ops/sec approx., which is 2.6x times increase in throughput. The performance increases with a bigger cluster. We observed linear scaling when increasing the number of server and client nodes. The system handled increased client load gracefully.

Strong Scaling of Clients - We keep the number of servers fixed and the scale up the number of clients. We observe the following throughput.
| Servers | Clients | Throughput (ops/sec) |
|---------|---------|----------------------|
| 2       | 2       | 7.25M                |
| 2       | 4       | 13.7M                |
| 2       | 6       | 14.4M                |
| 4       | 2       | 10.28M               |
| 4       | 4       | 19.43M               |


### Graphs & Visualizations

- **Scaling with Cluster Size**
  <br>
  ![WhatsApp Image 2025-09-07 at 8 02 44 PM](https://github.com/user-attachments/assets/4ac11870-210e-4188-9399-57e2df6bea41)


- **Resource Utilization**
  <br>
![WhatsApp Image 2025-09-07 at 7 34 45 PM (2)](https://github.com/user-attachments/assets/8d375455-8d04-4ca8-9880-b147c4e53627)


- **Final Results**
  <br>
![WhatsApp Image 2025-09-07 at 7 34 45 PM](https://github.com/user-attachments/assets/6ad94548-3511-412a-aa3a-a6357d3b96d7)
  
---

## Design

### Changes Made, Effects and Rationale

#### Transaction design

Each client generates a transaction id `TXID` which consists of (clientID, unixNano) and tracks the set of shard participants touched during the design. Keys are routed to servers via a consistent modulo partitioner; every `Get/Put` carries the TxID, and the client adds the destination server to the participants set.
The client acts as the coordinator and there is no explicit `Begin` phase. Once a client sends a Get/Put with a transaction ID that hasn't been seen before, a new transaction is initiated. At commit time, the client sends a `Commit` RPC to each participant.

`proto.go` has been modified to include `Commit` and `Abort` RPCs. These RPCs contain a boolean `Flag` so that a commit is correctly tracked only once by incrementing the `commits/aborts` counters even when multiple shards are involved. This flag is sent to the first server in the transaction's participants list. 
The `Get` and `Put` methods have been modified to include the transaction id. Lock conflicts are known through an RPC error `LOCK_DENIED`. We use a no-wait protocol to avoid deadlocks so a client immediately issues an `Abort` request on receiving the error.

Each server runs a simple lock-manager and a per-transaction record. A `TxRecord` holds a staged `writeSet` and a `readSet`; it’s created lazily on first touch (`Get/Put`). Concurrency control is strict two-phase locking at key granularity: a LockState for each key maintains a set of S-lock holders (`readers`) and a single X-lock holder (`writer`). `Get` first returns the caller’s uncommitted write if present (“read-your-writes”), otherwise it tries to take/keep an S-lock unless another transaction holds the X-lock. `Put` requires (or upgrades to) the X-lock—denying the request if there is another writer or any foreign readers—and stages the write in the transaction’s `writeSet`. On `Commit`, the server atomically installs all staged writes, then releases the X/S locks and drops the transaction record. `Abort` just releases locks and discards staged state.

If all the `Get/Put` operations in a transaction succeed, the servers hold the locks required to commit the transaction and the client sends a `Commit` request without the need for a `Prepare` phase.

#### Rationale and Strict Serializability

We aimed for the smallest mechanism that still guarantees serializability and read-your-writes within a transaction. Key-level strict 2PL is easier to reason about. The staged `writeSet` separates what will be committed from the map, so a `Get` can be served from the transaction's private state without extra versioning.
Conflicts are handled by immediate denial rather than waiting which eliminates deadlock handling logic. The client sends the `Commit/Abort` and handles errors eliminating the need for a coordinator.

Strict serializability is guaranteed because:
  1. Any schedule produced by strict 2PL is conflict-serializable. Locks ensure that any conflicting operations are ordered consistently and lock release at commit ensures no dirty reads or writes.
  2. A transaction cannot read uncommitted writes. It either reads from the kv map or its own write set. Once a transaction commits, its effects are permanent and visible.
  3. All writes become visible atomically at commit time. Consider a transaction T2 which starts after another transaction T1 commits. T1's writes will have already been applied by the time the client receives T1's commit acknowledgement. And T2 starts after T1's commit, therefore when T2 begins reading or writing, it sees the effects of T1. This preserves real-time order ensuring linearizability.
  4. Each transaction's commit is synchronous across all its participants (the client waits until all `Commit` RPCs succeed). The client considers the transaction to be committed only after all servers respond positively, ensuring global visibility is instantaneous from the client's perspective.

#### Trade-offs and alternatives considered

* There are a few costs for the simplicty. A crash or failure after some shards commit can leave a partial outcome across shards. True atomicity would require a commit to be persisted durably along with a recovery protocol. 
* Immediate denial on conflict preserves liveness by avoiding deadlocks but can increase abort rates on high contention. A waiting lock-table with timeouts, or deadlock detection would improve throughput under such scenarios.
* We guard the server state with a single mutex which is easier to reason about for correctness. But this doesn't allow parallelism. Unrelated keys still serialize on this mutex. A fine-grained locking approach where a lock manager does not serialize unrelated keys would scale better.
* The sharding and transaction ID generation are basic, this should be extended to hash distribution and monotonic ID generation for other workloads.






---

## Reproducibility

### Hardware Setup
We require between 4-8 m510 machines with 16-core Intel Xeon D-1548 processors clocked at 2.00GHz and 64GB of DDR4 memory clocked at 2400MT/s to reproduce the throughput results.

### Software Dependencies
- Install the Ubuntu 24.04 OS image on all the machines.
- Install go v1.25.0.
- Install and set up NFS storage and mount it at `/mnt/nfs`.
- Create a public-private key-pair and share the public key across all the machines.
- Set up ssh using the public key created in the previous step and enable key forwarding for password-less authentication.
- Optionally, also set a hostname alias of the form `node<id>` in the `~/.ssh/config` files for each machine provisioned.

### Build Instructions
Follow the steps below to clone the repo and build and run the code.
```bash
# Move to the path where the NFS storage has been mounted
cd /mnt/nfs
git clone <repo>
cd <repo>
./run-cluster.sh
```
Runs the cluster with default params.

To pass additional server and client configs and set the number of servers and clients, use
```bash
./run-cluster.sh SERVER_COUNT CLIENT_COUNT SERVER_CONFIG CLIENT_CONFIG
```

### Configuration Parameters

 - `batchSize` : Number of ops per batch before flush _(default = 8192)_
 - `batchTimeout` : Max time to wait before flushing a batch (in ms) _(default = 10ms)_
 - `brokersPerHost` :  Number of brokers per server _(default = 8)_
 - `generators` : Number of workload generator goroutines per client _(default = 8)_
 - `channelBuffer` : Size of the buffer for queuing ops by the broker _(default = 65536)_

The timeout is in milliseconds. Pass the client configuration params in the following manner
```bash
./run-cluster.sh 4 4 "" "--batchSize=16384 batchTimeout=5 --brokersPerHost=4"
```

---

## Reflections

### Lessons Learned

- **Batching** was the most powerful optimization for throughput.
- **Goroutine management** was very crucial: having too few goroutines, reduced the performance; whereas too many of them, induced contention and GC overhead.
- **Sync map** was not very helpful in this implementation.

### Optimizations that worked well

- Batching
- Brokers and multiple workload generators

### What Didn’t Work

- **Single-request RPCs** were far too slow.
- Concurrent Map did not boost the performance as expected as the locks are held for a very short amount of time.

### Future Directions

- Integrate **gRPC** or **raw TCP** for lower RPC latency.
- Implement a **lock-free map** to reduce write-side contention and improve scalability.
- Substitute the default `gob` encoding with a **custom encoding** for a fixed workload.

### Team Contributions

- **Rajavardhan Reddy Siriganagiri**: Client code & batch strategy  
- **Siddharth Kapoor**: Server implementation & profiling 
- **Neha Bhat**: Documentation, tuning, and result analysis
