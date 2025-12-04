#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_ROOT="${ROOT}/bench_logs_repl_2"

# rm -rf $LOG_ROOT

#######################################################################
# GROUP A — Main Looping Parameters
#######################################################################
WORKLOADS=("YCSB-C")
WAIT_POLICIES=("woundwait")
REPEATS=3

#######################################################################
# GROUP B — Non-looping parameters (one varied at a time)
#######################################################################

### Baseline values
BASE_SERVERS=2
BASE_THREADS=10
BASE_THETA=0.99
BASE_OPS=3

### Parameters to vary independently
SERVERS_LIST=(1 2 3)
THREADS_LIST=(20)
THETA_LIST=(0 0.2 0.4 0.6 0.8 0.99)
OPS_LIST=(3 20)
#######################################################################

SSH_OPTS="-o StrictHostKeyChecking=no"
SSH="ssh ${SSH_OPTS}"

cluster_size() { /usr/local/etc/emulab/tmcc hostnames | wc -l; }
AVAILABLE=$(cluster_size)

cleanup() {
    echo "Cleaning up processes on all nodes..."
    for node in "${ALL_NODES[@]:-}"; do
        echo "Cleaning up processes on $node..."
        ssh $SSH_OPTS $node "pkill -e -f kvs || true" 2>/dev/null || true
    done
    echo "Cleanup complete."
}
# (servernowait|serverwaitdie|serverwoundwait|client)
trap cleanup EXIT INT TERM

echo "Building project…"
for wait_policy in "${WAIT_POLICIES[@]}"; do
    make -B SERVER_POLICY="$wait_policy" build-server
done
make build-client

#######################################################################
# Helper: Thread distribution across clients
#######################################################################
calculate_threads() {
    local total=$1
    local clients=$2
    local -n arr=$3
    local base=$(( total / clients ))
    local remainder=$(( total % clients ))

    arr=()
    for ((i=0; i<clients; i++)); do
        if (( i == clients - 1 )); then
            arr+=($((base + remainder)))
        else
            arr+=($base)
        fi
    done
}

#######################################################################
# Core Routine — runs one experiment configuration through Group A
#######################################################################
run_experiment_group() {
    local exp_name="$1"
    local servers="$2"
    local threads="$3"
    local theta="$4"
    local ops="$5"

    local clients=$((4 - servers))

    echo "=== Running Group: $exp_name ==="
    echo "Servers=$servers Clients=$clients Threads=$threads Theta=$theta Ops=$ops"
    echo

    # Build node lists
    SERVER_NODES=()
    CLIENT_NODES=()
    for ((i=0; i<servers; i++)); do SERVER_NODES+=("node$i"); done
    for ((i=servers; i<servers+clients; i++)); do CLIENT_NODES+=("node$i"); done
    ALL_NODES=("${SERVER_NODES[@]}" "${CLIENT_NODES[@]}")

    # Distribute threads
    declare -a CLIENT_THREADS
    calculate_threads "$threads" "$clients" CLIENT_THREADS

    # Group A permutations
    for workload in "${WORKLOADS[@]}"; do
    for wait_policy in "${WAIT_POLICIES[@]}"; do
    for repeat in $(seq 1 $REPEATS); do

        echo "Cleaning up processes..."
        cleanup
        echo

        LOG_DIR="$LOG_ROOT/$exp_name/servers_${servers}_clients_${clients}/threads_${threads}/theta_${theta}/ops_${ops}/workload_${workload}/wait_${wait_policy}"
        mkdir -p "$LOG_DIR"
        # REPEAT_LOG="$LOG_DIR/repeat_${repeat}.log"

        echo ">>> ($exp_name) workload=$workload wait=$wait_policy repeat=$repeat"

        # server_args="--policy $wait_policy"
        server_args=""

        # Per-client args
        declare -a client_args
        for ((c=0; c<clients; c++)); do
            client_args[$c]="--threads ${CLIENT_THREADS[$c]} --workload $workload --theta $theta --opsPerTx $ops"
        done

        # Start servers
        for node in "${SERVER_NODES[@]}"; do
            echo $server_args
            ssh $SSH_OPTS $node "${ROOT}/bin/kvsserver$wait_policy $server_args > "$LOG_DIR/run-$repeat-kvsserver$wait_policy-$node.log" 2>&1 &"
        done
        sleep 2

        SERVER_HOSTS=$(IFS=, ; echo "${SERVER_NODES[*]/%/:8080}")

        # Start clients
        CLIENT_PIDS=()
        for ((i=0; i<clients; i++)); do
            node="${CLIENT_NODES[$i]}"
            args="${client_args[$i]}"

            echo $args
            ssh $SSH_OPTS $node "${ROOT}/bin/kvsclient -hosts $SERVER_HOSTS $args > "$LOG_DIR/run-$repeat-kvsclient-$node.log" 2>&1" &
            CLIENT_PIDS+=($!)
        done

        for pid in "${CLIENT_PIDS[@]}"; do wait $pid || true; done

    done; done; done
}

#######################################################################
# Run Experiment Groups — one parameter varied at a time
#######################################################################

cleanup

# ### 1. Vary servers
# for val in "${SERVERS_LIST[@]}"; do
#     run_experiment_group "vary_servers_$val" "$val" "$BASE_THREADS" "$BASE_THETA" "$BASE_OPS"
# done

### 2. Vary total threads
for val in "${THREADS_LIST[@]}"; do
    run_experiment_group "vary_threads_$val" "$BASE_SERVERS" "$val" "$BASE_THETA" "$BASE_OPS"
done

# ### 3. Vary theta
# for val in "${THETA_LIST[@]}"; do
#     run_experiment_group "vary_theta_$val" "$BASE_SERVERS" "$BASE_THREADS" "$val" "$BASE_OPS"
# done

# ### 4. Vary opsPerTx
# for val in "${OPS_LIST[@]}"; do
#     run_experiment_group "vary_ops_$val" "$BASE_SERVERS" "$BASE_THREADS" "$BASE_THETA" "$val"
# done

echo "All experiment groups completed."
