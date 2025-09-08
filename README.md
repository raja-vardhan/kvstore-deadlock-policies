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

#### Sharding

The original skeleton code sent the requests to a single server. Distribute the key across the available servers using the modulo operator (`key % num_of_servers`) for distributing the load. A key always goes to the same server.
For a 4 node setup with 2 clients and 2 servers, the performance jumps by 90-100%.
Equal key distribution does not mean equal load distribution but this works for 2 servers due to the nature of YCSB-B workload (The load distribution is approximately 55% on one server and 45% on the other).

#### Concurrent map

Since the default map implementation serializes every read/write, we replace it with a popular concurrent map implementation **[[concurrent-map](https://github.com/orcaman/concurrent-map)]**.
Instead of a single big map with one lock, this map is split into **32 shards**.

This reduces lock contention:
  - Two goroutines modifying keys in different shards → no blocking.
  - Two goroutines modifying keys in the same shard → they block each other.

This ensures that not all writes are serialized.
This had a negligible effect on performance (< 10%) when each operation is executed in a separate goroutine as the locks are held for a very short amount of time.

#### Batching

We queue requests at the client until we accumulate a specific number of requests (configurable through the `batchSize` flag). This batch is sent to the server for processing.
Without batching, each request to the server executes in a separate goroutine. This adds huge scheduling overhead on the server side. Also, each request has to make a round trip on the network and adds a TCP/HTTP header.
Batching amortizes the cost of system calls, network round trips. It also reduces the scheduling overhead on the server.
Due to the skewed distribution of R/W operations, we also introduce a timeout (configurable through `batchTimeout` flag). Cold keys don't fill up a batch quickly and this adds to their latency. If a batch doesn't get filled after the timeout, it is still flushed to the server.

This improved the throughput by **5x**. It also reduced the CPU utilization on the server (less scheduling + system call overhead). 

#### Brokers and multiple workload generators

The idea of brokers is inspired from thread pools. We separate the generation of the workload from its execution. 
There is scope for concurrency when using synchronous RPC calls as the client is blocked waiting for the response to arrive. 
A broker's job is to batch requests on the client and send them to the server and process the response. Each client creates multiple brokers per server (configurable through the `brokersPerHost` flag). This ensures that when a broker is waiting for the response, another broker can execute the next request.
Each broker maintains its own buffered channel (its size is configurable through the `channelBuffer` flag). We first determine the server that the key should go to using the modulo operator and then place the operation on one of the brokers' channels. We assign the operations in a round-robin fashion and block when all the channels are full.

With multiple brokers available at a given time, we also increased the number of workload generators (configurable through `generators` flag). The combined effect of using brokers and multiple workload generators increased the throughput to the final number of _**7.2 million ops/sec**_.

### Trade-offs & Alternatives

Below are a number of alternative strategies with tradeoffs. They could not be implemented due to either time constraints or complexity or minimal performance gain.

#### Asynchronous RPC calls and result handling

Making the RPC calls asynchronous will ensure that a single broker can send a batch and immediately be ready to create the next batch. The results from the asynchronous RPC calls can be handled in a separate goroutine. 
It is important to restrict the number of result handlers so that too many responses don't put backpressure and add scheduling overhead. This can be implemented using semaphores and a queue for the responses.
This eliminates the need for too many brokers as a broker can send the next batch immediately.

#### Multiple client-server TCP connections

Each broker can have its own TCP connection instead of a single connection per client-server. This had negligible performance gain as the network utilization was very high.

#### Others

HTTP adds overhead and latency due to headers, TLS handshake etc. We can use raw TCP directly but this adds complexity to implementation. We can also substitute the default `gob` encoding with a custom encoding for a fixed workload.

### Performance Bottleneck Analysis

#### High client CPU utilization

The client CPU utilization is >90% on using a combination of 4 clients and 4 servers while the server utilization is under 60%. The bottleneck pointed to high GC overhead and syscalls.
Since there were a lot of configurable parameters like `batchSize, batchTimeout, channelBuffer, brokersPerHost`, this pointed to tuning the above parameters using sweeping, instead of a trial and error, to minimize client utilization while maintaining throughput. We could also switch to other serialization methods like Protobuf.

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

### Configuration Parameters
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
- Concurrent Map did not boost the performance as expected since the operations are executed in separate goroutines as the locks are held for a very short amount of time.

### Future Directions

- Integrate **gRPC** or **raw TCP** for lower RPC latency.
- Implement a **sharded** or **lock-free map** to reduce write-side contention and improve scalability.
- Substitute the default `gob` encoding with a **custom encoding** for a fixed workload.

### Team Contributions

- **Rajavardhan Reddy Siriganagiri**: Client code & batch strategy  
- **Siddharth Kapoor**: Server implementation & profiling 
- **Neha Bhat**: Documentation, tuning, and result analysis
