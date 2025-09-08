# High-Performance Key-Value Store

## Results

### Final Throughput Numbers
In our implementation of the High-Throughput Key-Value Store, we achieved a peak throughput of **19.43M ops/sec** (19435544 ops/s) using 8 CloudLab `m510` nodes and the YCSB-B workload (`95% gets`, `5% puts`, θ = 0.99). Our system was tested using the provided workload generator, maintaining full workload integrity and linearizability under normal operation.

### Hardware Utilization
- **CPU**: ~65% usage per core on server nodes during peak load
- **Memory**: ~1 GB per node
- **Network**: Sustained ~9.5 Gbps throughput over 10 Gbps interfaces

### Scaling Characteristics
At 4 nodes(2 client, 2 server) we achieve 7.2M ops/s. When we scale the cluster to 8 nodes, we achieve 19M ops/s approx., which is 2.6x times increase in throughput. The performance increases with a bigger cluster. We observed linear scaling when increasing the number of server and client nodes. The system handled increased client load gracefully.

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

Our solution is a distributed batched-RPC key-value store built in Go, composed of:
- **Clients**: Generate load using Zipfian-distributed YCSB workloads and batch requests for performance.
- **Servers**: Respond to batch RPCs and maintain an in-memory concurrent map.

### Changes Made & Effects
*(Describe modifications to baseline client/server code and their impact on throughput.)*

### Rationale for Choices
*(Why you picked batching, brokers, workload generators, etc. and how they improved scalability.)*

### Trade-offs & Alternatives
*(Other designs you considered, why you didn’t use them, and trade-offs between simplicity vs. performance.)*

### Performance Bottleneck Analysis
*(Explain how you identified bottlenecks — e.g., RPC latency, CPU utilization, GC pressure — and the steps taken to address them.)*

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
```

---

## Reflections

### Lessons Learned

- **Batching** was the most powerful optimization for throughput.
- **Goroutine management** was crucial: too few, reduced performance; too many, induced contention and GC overhead.
- **Go’s runtime** made rapid prototyping easy, but careful tuning was required to sustain performance under high concurrency.

### What Didn’t Work

- **Single-request RPCs** were far too slow.

### Future Directions

- Integrate **gRPC** or **raw TCP** for lower RPC latency.
- Implement a **sharded** or **lock-free map** to reduce write-side contention and improve scalability.
- Explore **advanced networking** options like **RDMA** to bypass kernel networking stacks for ultra-low latency.

### Team Contributions

- **Rajavardhan Reddy Siriganagiri**: Client code & batch strategy  
- **Siddharth Kapoor**: Server implementation & profiling 
- **Neha Bhat**: Documentation, tuning, and result analysis
