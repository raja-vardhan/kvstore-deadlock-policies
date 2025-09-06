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

## Design

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

