# Evaluating 2PL Lock Policies in a Distributed Key-Value Store

This repository contains a **distributed transactional key-value store** implemented in Go, designed to evaluate the performance and behavior of different **Two-Phase Locking (2PL) deadlock avoidance strategies** under realistic workloads.

The project was developed as part of **CS 6450 – Distributed Systems** at the **University of Utah** and focuses on comparing three classic lock management policies:

* **No-Wait**
* **Wait-Die**
* **Wound-Wait**

All policies are implemented under a **common client and RPC protocol**, enabling fair, apples-to-apples comparison across throughput, abort rate, and latency.

---

## Key Features

* **Distributed Transactions**

  * Strict serializability via **2PL + 2PC**
  * Sharded key-value store with RPC communication
* **Multiple Deadlock Avoidance Policies**

  * No-Wait (baseline)
  * Wait-Die (timestamp-based waiting)
  * Wound-Wait (preemptive, priority-based)
* **Realistic Workloads**

  * YCSB-A, YCSB-B, YCSB-C
  * Bank-transfer workload
* **Detailed Metrics**

  * Throughput (commit rate)
  * Abort rate
  * Latency (avg, median, p95, p99)
* **Configurable Experiments**

  * Number of shards
  * Client threads
  * Transaction length
  * Workload type
  * Zipfian skew (θ)

---

## System Overview

### Architecture

* **Clients**

  * Multiple worker goroutines generate and execute transactions
  * Each transaction may span multiple shards
  * Automatic retry on abort
* **Servers**

  * Sharded KV store
  * Per-key lock manager
  * Timestamp-ordered wait queues
* **Coordination**

  * Two-Phase Commit (2PC)
  * Prepared transactions are immune to wounding

---

##  Locking Policies

### 1. No-Wait

* Immediate abort on any lock conflict
* Simplest implementation
* Performs poorly under contention due to excessive retries

### 2. Wait-Die

* Transactions are timestamped
* Older transactions may wait for younger ones
* Younger transactions abort immediately
* Includes:

  * Priority wait queues
  * Special handling for S→X lock upgrades

### 3. Wound-Wait

* Older transactions preempt (wound) younger ones
* Younger transactions wait for older holders
* Optimizations:

  * Early lock release on wounding
  * Lazy abort propagation
  * Prepared transactions cannot be wounded

---

## Experimental Results (Summary)

Key findings from extensive experiments:

* **Wound-Wait**

  * Lowest abort rate across all workloads
  * Best performance under high contention
  * Higher tail latency (p95/p99)
* **Wait-Die**

  * Similar throughput to No-Wait for short transactions
  * Suffers under long or write-heavy workloads
* **No-Wait**

  * Lowest average latency
  * Extremely inefficient due to high abort rates

These results are discussed in detail in the final report, including plots across:

* Transaction length
* Contention levels (Zipf θ)
* Client load
* Number of shards 

---

## Running the System

### Prerequisites

* Go (1.20+ recommended)
* Linux
* Multiple machines or VMs (for multi-shard experiments)

### Configuration Parameters
To pass additional server and client configs and set the number of servers and clients, use
```bash
./run-cluster.sh SERVER_COUNT CLIENT_COUNT SERVER_CONFIG CLIENT_CONFIG
```
To pass theta parameter, use
```bash
./run-cluster.sh SERVER_COUNT CLIENT_COUNT "" "-theta 0" #For theta=0
./run-cluster.sh SERVER_COUNT CLIENT_COUNT "" "-theta 0.99" #For theta=0.99
```
To pass workload parameter, use
```bash
./run-cluster.sh SERVER_COUNT CLIENT_COUNT "" "-workload xfer"
```
---

## Metrics Collection

* Latency is measured **per committed transaction**
* A dedicated aggregator goroutine computes:

  * Average
  * Median
  * 95th percentile
  * 99th percentile
* Abort and commit counts are recorded continuously

---

## Repository Structure (High-Level)

```
├── bench-cluster.sh         # Automates end-to-end benchmarking across a multi-node cluster
├── kvs/                     # Core key-value store implementation
│   ├── client/              # Transactional client logic (workers, retries, RPC stubs)
│   ├── loadgen.go           # Workload generator (YCSB, bank workload, Zipfian skew)
│   ├── proto.go             # Shared RPC protocol definitions and transaction metadata
│   ├── server/              # Sharded KV server, lock manager, and 2PL/2PC logic
│   └── utils/               # Common utilities
├── Makefile                 # Build, run, and experiment
├── report-tput.py           # Parses experiment logs and plots throughput/abort metrics
└── run-cluster.sh           # Launches client and server processes across the cluster

```

---

## Reference

For a complete explanation of the design, algorithms, and experimental evaluation, see:

[**Evaluating 2PL Lock Policies in a Distributed Key-Value Store**](CS6450_Final_Project_Report.pdf)

---