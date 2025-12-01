#!/bin/bash

# run-cluster.sh - Run distributed key-value store cluster with parameterized tests

set -euo pipefail


# Function to display usage
usage() {
    echo "Usage: $0 [server_count] [client_count] [server_args] [client_args]"
    echo "  server_count: Number of server nodes to use [optional - defaults to half of available nodes]"
    echo "  client_count: Number of client nodes to use [optional - defaults to remaining nodes]"
    echo "  server_args:  Arguments to pass to server processes (quoted string) [optional]"
    echo "  client_args:  Arguments to pass to client processes (quoted string) [optional]"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Auto-split available nodes"
    echo "  $0 2                                  # 2 servers, rest as clients"
    echo "  $0 2 3                               # 2 servers, 3 clients"
    echo "  $0 2 3 \"-port 8080\""
    echo "  $0 2 3 \"-port 8080\" \"-workload YCSB-A -secs 30\""
    exit 1
}

# Check for help options
for arg in "$@"; do
    case "$arg" in
        help|-h|--help|-help)
            usage
            ;;
    esac
done

# Configuration
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_ROOT="${ROOT}/log_test"

function cluster_size() {
    /usr/local/etc/emulab/tmcc hostnames | wc -l
}

# Get available node count first to determine defaults
AVAILABLE_COUNT=$(cluster_size)
echo "Available nodes: $AVAILABLE_COUNT"

SERVER_COUNTS=(2)
TOTAL_THREADS=(10)
WORKLOADS=("YCSB-A")
THETAS=(0.99)
OPSPERTX=(3 20)
WAIT_POLICIES=("wd")
REPEATS=1

SSH_OPTS="-o StrictHostKeyChecking=no"
SSH="ssh ${SSH_OPTS}"

# Function to calculate threads per client
calculate_threads() {
    local total=$1
    local clients=$2
    local -n threads_arr=$3
    local base_threads=$(( total / clients ))
    local remainder=$(( total % clients ))
    threads_arr=()
    for ((i=0; i<clients; i++)); do
        if (( i == clients - 1 )); then
            threads_arr+=($(( base_threads + remainder )))
        else
            threads_arr+=($base_threads)
        fi
    done
}

# Cleanup function
cleanup() {
    echo "Cleaning up processes on all nodes..."
    for node in "${SERVER_NODES[@]}" "${CLIENT_NODES[@]}"; do
        echo "Cleaning up processes on $node..."
        ${SSH} $node "pkill -f 'kvs(server|client)' || true" 2>/dev/null || true
    done
    echo "Cleanup complete."
}
trap cleanup EXIT INT TERM

echo "Initial cluster cleanup..."
cleanup

echo "Building the project..."
make
echo

# Iterate over all parameter combinations
for servers in "${SERVER_COUNTS[@]}"; do
    clients=$(( AVAILABLE_COUNT - servers ))
    if (( clients < 1 )); then continue; fi

    for total_threads in "${TOTAL_THREADS[@]}"; do
        declare -a client_threads
        calculate_threads "$total_threads" "$clients" client_threads

        for workload in "${WORKLOADS[@]}"; do
            for theta in "${THETAS[@]}"; do
                for ops in "${OPSPERTX[@]}"; do
                    for wait_policy in "${WAIT_POLICIES[@]}"; do
                        # Build server and client nodes arrays
                        SERVER_NODES=()
                        for ((i=0; i<servers; i++)); do SERVER_NODES+=("node$i"); done
                        CLIENT_NODES=()
                        for ((i=servers; i<servers+clients; i++)); do CLIENT_NODES+=("node$i"); done

                        # Build server args
                        server_args=()
                        for ((s=0; s<servers; s++)); do
                            # server_args+=("--waitPolicy $wait_policy")
                            server_args+=("")
                        done

                        # Build client args
                        client_args=()
                        for ((c=0; c<clients; c++)); do
                            client_args+=("--threads ${client_threads[c]} --workload $workload --theta $theta --opsPerTx $ops")
                        done

                        # Repeat the test REPEATS times
                        for repeat in $(seq 1 $REPEATS); do
                            # Combined server+client directory for logs
                            TS=$(date +"%Y%m%d-%H%M%S")
                            log_dir="$LOG_ROOT/servers_${servers}_clients_${clients}/threads_$total_threads/workload_$workload/theta_$theta/ops_$ops/wait_$wait_policy/repeat_$repeat"
                            mkdir -p "$log_dir"
                            repeat_log="$log_dir/run_info.log"
                            echo "==== Running Test ====" | tee "$repeat_log"
                            echo "Servers: $servers, Clients: $clients, Repeat: $repeat" | tee -a "$repeat_log"
                            echo "Client Threads: ${client_threads[*]}" | tee -a "$repeat_log"
                            echo "Workload: $workload, Theta: $theta, OpsPerTx: $ops, Wait: $wait_policy" | tee -a "$repeat_log"
                            echo "Server Nodes: ${SERVER_NODES[*]}" | tee -a "$repeat_log"
                            echo "Client Nodes: ${CLIENT_NODES[*]}" | tee -a "$repeat_log"
                            echo "Server Args: ${server_args[*]}" | tee -a "$repeat_log"
                            for ((c=0; c<clients; c++)); do
                                echo "Client $((c+1)) Args: ${client_args[c]}" | tee -a "$repeat_log"
                            done

                            # Start servers
                            for idx in "${!SERVER_NODES[@]}"; do
                                node="${SERVER_NODES[$idx]}"
                                args="${server_args[$idx]}"
                                ${SSH} $node "${ROOT}/bin/kvsserver $args > \"$log_dir/kvsserver-$node.log\" 2>&1 &"
                            done
                            sleep 2

                            # Start clients
                            CLIENT_PIDS=()
                            SERVER_HOSTS=$(IFS=, ; echo "${SERVER_NODES[*]/%/:8080}")
                            for idx in "${!CLIENT_NODES[@]}"; do
                                node="${CLIENT_NODES[$idx]}"
                                args="${client_args[$idx]}"
                                CLIENT_MARKER="kvsclient-${TS}-${node}"
                                ${SSH} $node "exec -a '$CLIENT_MARKER' ${ROOT}/bin/kvsclient -hosts $SERVER_HOSTS $args > \"$log_dir/kvsclient-$node.log\" 2>&1" &
                                CLIENT_PIDS+=($!)
                            done

                            # Wait for clients
                            for pid in "${CLIENT_PIDS[@]}"; do wait $pid 2>/dev/null || true; done
                            echo "All clients finished for repeat $repeat" | tee -a "$repeat_log"

                            # Cleanup servers before next repeat
                            cleanup
                        done

                    done
                done
            done
        done
    done
done

echo "All tests complete. Logs in $LOG_ROOT"
