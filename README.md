# High-Throughput KV Store Benchmark Client

## Results

### Final Throughput Numbers
*(Summarize measured throughput in ops/s, compare to grading scale, and note which grade threshold was achieved.)*

### Hardware Utilization
*(CPU usage, memory footprint, network utilization, any observed bottlenecks.)*

### Scaling Characteristics
*(Explain how performance changed with cluster size and client load. Show results for both small- and large-scale runs.)*

### Graphs & Visualizations
- **Throughput vs. Offered Load**  
  *(Insert a graph showing ops/s achieved as load increases.)*

- **Scaling with Cluster Size**  
  *(Insert a graph showing how throughput changes as number of servers increases.)*

- **Resource Utilization**  
  *(Insert CPU, memory, and network utilization graphs.)*

### Performance Grading Scale (YCSB-B, θ = 0.99)


---

# Design

## Changes Made, Effects and Rationale

### Sharding

The original skeleton code sent the requests to a single server. Distribute the key across the available servers using the modulo operator (`key % num_of_servers`) for distributing the load. A key always goes to the same server.
For a 4 node setup with 2 clients and 2 servers, the performance jumps by 90-100%.
Equal key distribution does not mean equal load distribution but this works for 2 servers due to the nature of YCSB-B workload (The load distribution is approximately 55% on one server and 45% on the other).


### Concurrent map

Since the default map implementation serializes every read/write, we replace it with a popular concurrent map implementation **[[concurrent-map](https://github.com/orcaman/concurrent-map)]**.
Instead of a single big map with one lock, this map is split into **32 shards**.

This reduces lock contention:
  - Two goroutines modifying keys in different shards → no blocking.
  - Two goroutines modifying keys in the same shard → they block each other.

This ensures that not all writes are serialized.
This had a negligible effect on performance (< 10%) when each operation is executed in a separate goroutine as the locks are held for a very short amount of time.

### Batching

We queue requests at the client until we accumulate a specific number of requests (configurable through the `batchSize` flag). This batch is sent to the server for processing.
Without batching, each request to the server executes in a separate goroutine. This adds huge scheduling overhead on the server side. Also, each request has to make a round trip on the network and adds a TCP/HTTP header.
Batching amortizes the cost of system calls, network round trips. It also reduces the scheduling overhead on the server.
Due to the skewed distribution of R/W operations, we also introduce a timeout (configurable through `batchTimeout` flag). Cold keys don't fill up a batch quickly and this adds to their latency. If a batch doesn't get filled after the timeout, it is still flushed to the server.

This improved the throughput by **5x**. It also reduced the CPU utilization on the server (less scheduling + system call overhead). 

### Brokers and multiple workload generators

The idea of brokers is inspired from thread pools. We separate the generation of the workload from its execution. 
There is scope for concurrency when using synchronous RPC calls as the client is blocked waiting for the response to arrive. 
A broker's job is to batch requests on the client and send them to the server and process the response. Each client creates multiple brokers per server (configurable through the `brokersPerHost` flag). This ensures that when a broker is waiting for the response, another broker can execute the next request.
Each broker maintains its own buffered channel (its size is configurable through the `channelBuffer` flag). We first determine the server that the key should go to using the modulo operator and then place the operation on one of the brokers' channels. We assign the operations in a round-robin fashion and block when all the channels are full.

With multiple brokers available at a given time, we also increased the number of workload generators (configurable through `generators` flag). The combined effect of using brokers and multiple workload generators increased the throughput to the final number of _**7.2 million ops/sec**_.

## Trade-offs & Alternatives

Below are a number of alternative strategies with tradeoffs. They could not be implemented due to either time constraints or complexity or minimal performance gain.

### Asynchronous RPC calls and result handling

Making the RPC calls asynchronous will ensure that a single broker can send a batch and immediately be ready to create the next batch. The results from the asynchronous RPC calls can be handled in a separate goroutine. 
It is important to restrict the number of result handlers so that too many responses don't put backpressure and add scheduling overhead. This can be implemented using semaphores and a queue for the responses.
This eliminates the need for too many brokers as a broker can send the next batch immediately.

### Multiple client-server TCP connections

Each broker can have its own TCP connection instead of a single connection per client-server. This had negligible performance gain as the network utilization was very high.

### Others

HTTP adds overhead and latency due to headers, TLS handshake etc. We can use raw TCP directly but this adds complexity to implementation. We can also substitute the default `gob` encoding with a custom encoding for a fixed workload.


## Performance Bottleneck Analysis

### High client CPU utilization

The client CPU utilization is >90% on using a combination of 4 clients and 4 servers while the server utilization is under 60%. The bottleneck pointed to high GC overhead and syscalls.
Since there were a lot of configurable parameters like `batchSize, batchTimeout, channelBuffer, brokersPerHost`, this pointed to tuning the above parameters using sweeping, instead of a trial and error, to minimize client utilization while maintaining throughput. We could also switch to other serialization methods like Protobuf.

---

## Reproducibility

### Hardware Setup
*(Specify machine specs — CPU cores, memory, NIC speed, number of nodes, etc.)*

### Software Dependencies
- Go (version)  
- Other libraries (if any)  
- OS environment  

### Build Instructions
```bash
git clone <repo>
cd kvs/client
go build -o client main.go

