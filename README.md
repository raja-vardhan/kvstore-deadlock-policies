# Distributed Transactions

### Hardware Setup
We require between 4 m510 machines with 16-core Intel Xeon D-1548 processors clocked at 2.00GHz and 64GB of DDR4 memory clocked at 2400MT/s to reproduce the throughput results.

### Software Dependencies
- Install the Ubuntu 24.04 OS image on all the machines.
- Install go v1.25.0.
- Install and set up NFS storage and mount it at `/mnt/nfs`.
- Create a public-private key-pair and share the public key across all the machines.
- Set up ssh using the public key created in the previous step and enable key forwarding for password-less authentication.
- Optionally, also set a hostname alias of the form `node<id>` in the `~/.ssh/config` files for each machine provisioned.

### Build Instructions
Follow the steps below to clone the repo and build and run the full benchmark for the code.
```bash
# Move to the path where the NFS storage has been mounted
cd /mnt/nfs
git clone <repo>
cd <repo>
./bench-cluster.sh
```
Runs the full suite of benchmarks on the cluster.

Follow the steps below to build and run the code for a single iteration.
```bash
# Move to the path where the NFS storage has been mounted
cd /mnt/nfs
git clone <repo>
cd <repo>
./run-cluster.sh --policy=<"nowait"|"waitdie"|"woundwait">
```
Runs the full suite of benchmarks on the cluster.

### Configuration Parameters
To pass additional server and client configs and set the number of servers and clients, use
```bash
./run-cluster.sh SERVER_COUNT CLIENT_COUNT SERVER_CONFIG CLIENT_CONFIG --policy=<"nowait"|"waitdie"|"woundwait">
```
To pass additional theta values, use
```bash
./run-cluster.sh SERVER_COUNT CLIENT_COUNT "" "-theta 0" --policy=<"nowait"|"waitdie"|"woundwait"> #For theta=0
./run-cluster.sh SERVER_COUNT CLIENT_COUNT "" "-theta 0.99" --policy=<"nowait"|"waitdie"|"woundwait"> #For theta=0.99
```
To pass additional workload parameter, use
```bash
./run-cluster.sh SERVER_COUNT CLIENT_COUNT "" "-workload xfer" --policy=<"nowait"|"waitdie"|"woundwait">
```
To change the number of client gorountines on each machine, use
```bash
./run-cluster.sh SERVER_COUNT CLIENT_COUNT "" "-threads 20" --policy=<"nowait"|"waitdie"|"woundwait">
```
To change the number of operations per transaction, use
```bash
./run-cluster.sh SERVER_COUNT CLIENT_COUNT "" "-opsPerTx 10" --policy=<"nowait"|"waitdie"|"woundwait">
```
To change the runtime of the clients, use
```bash
./run-cluster.sh SERVER_COUNT CLIENT_COUNT "" "-secs 40" --policy=<"nowait"|"waitdie"|"woundwait">
```
---
