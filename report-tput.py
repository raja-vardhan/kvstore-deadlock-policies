#!/usr/bin/env python3

import os
import glob
import statistics
import re

LOG_DIR = "./logs/latest"
RESULTS_FILE = os.path.join(LOG_DIR, "results.txt")

SERVER_SUMMARIES = []
CLIENT_SUMMARIES = []

# --------------------------------------------------------------------
# Helper regex for client summary lines
# --------------------------------------------------------------------
SUMMARY_RE = re.compile(
    r"CLIENT-SUMMARY:\s*"
    r"attempts=(?P<attempts>\d+)\s*"
    r"commits=(?P<commits>\d+)\s*"
    r"aborts=(?P<aborts>\d+)\s*"
    r"samples=(?P<samples>\d+)\s*"
    r"avg_ms=(?P<avg>[\d.]+)\s*"
    r"median_ms=(?P<median>[\d.]+)\s*"
    r"p95_ms=(?P<p95>[\d.]+)\s*"
    r"p99_ms=(?P<p99>[\d.]+)"
)

# --------------------------------------------------------------------
# Parse server throughput logs
# --------------------------------------------------------------------
server_files = sorted(glob.glob(os.path.join(LOG_DIR, "kvsserver-*.log")))

if not server_files:
    print("No server log files found.")
else:
    print("=== Server Throughput Summary ===")

total_commits = 0.0
total_aborts = 0.0

for log_path in server_files:
    node = (
        os.path.basename(log_path)
        .removeprefix("kvsserver-")
        .removesuffix(".log")
    )

    commits = []
    aborts = []

    with open(log_path) as f:
        for line in f:
            if "commits/s" in line:
                parts = line.strip().split()
                try:
                    commits.append(float(parts[1]))
                except:
                    pass
            elif "aborts/s" in line:
                parts = line.strip().split()
                try:
                    aborts.append(float(parts[1]))
                except:
                    pass

    if commits:
        median_c = statistics.median(sorted(commits))
        total_commits += median_c
        print(f"{node}: {median_c:.0f} commits/s")
    else:
        print(f"{node}: no commits/s data")

    if aborts:
        median_a = statistics.median(sorted(aborts))
        total_aborts += median_a
        print(f"{node}: {median_a:.0f} aborts/s")
    else:
        print(f"{node}: no aborts/s data")

print()
print(f"TOTAL commits/s: {total_commits:.0f}")
print(f"TOTAL aborts/s:  {total_aborts:.0f}")
print()

SERVER_SUMMARIES.append(f"TOTAL commits/s: {total_commits:.0f}")
SERVER_SUMMARIES.append(f"TOTAL aborts/s:  {total_aborts:.0f}")


# --------------------------------------------------------------------
# Parse client CLIENT-SUMMARY lines
# --------------------------------------------------------------------
client_files = sorted(glob.glob(os.path.join(LOG_DIR, "kvsclient-*.log")))

if not client_files:
    print("No client log files found.")
else:
    print("=== Client Latency Summary ===")

    for log_path in client_files:
        cid = os.path.basename(log_path).removeprefix("kvsclient-").removesuffix(".log")

        with open(log_path) as f:
            for line in f:
                m = SUMMARY_RE.search(line)
                if m:
                    d = {k: float(v) for k, v in m.groupdict().items()}
                    CLIENT_SUMMARIES.append((cid, d))
                    print(
                        f"client-{cid}: "
                        f"samples={int(d['samples'])} "
                        f"avg={d['avg']:.3f}ms "
                        f"median={d['median']:.3f}ms "
                        f"p95={d['p95']:.3f}ms "
                        f"p99={d['p99']:.3f}ms "
                        f"commits={int(d['commits'])} "
                        f"aborts={int(d['aborts'])}"
                    )
                    break

    # Aggregate summary
    if CLIENT_SUMMARIES:
        agg = {
            "samples": sum(d["samples"] for _, d in CLIENT_SUMMARIES),
            "avg": statistics.mean(d["avg"] for _, d in CLIENT_SUMMARIES),
            "median": statistics.mean(d["median"] for _, d in CLIENT_SUMMARIES),
            "p95": statistics.mean(d["p95"] for _, d in CLIENT_SUMMARIES),
            "p99": statistics.mean(d["p99"] for _, d in CLIENT_SUMMARIES),
            "commits": sum(d["commits"] for _, d in CLIENT_SUMMARIES),
            "aborts": sum(d["aborts"] for _, d in CLIENT_SUMMARIES),
        }

        print("\nAggregated client latency:")
        print(
            f"samples={int(agg['samples'])} "
            f"avg={agg['avg']:.3f}ms "
            f"median={agg['median']:.3f}ms "
            f"p95={agg['p95']:.3f}ms "
            f"p99={agg['p99']:.3f}ms "
            f"commits={int(agg['commits'])} "
            f"aborts={int(agg['aborts'])}"
        )


# --------------------------------------------------------------------
# Write results to results.txt
# --------------------------------------------------------------------
with open(RESULTS_FILE, "w") as out:
    out.write("=== Server Summary ===\n")
    for line in SERVER_SUMMARIES:
        out.write(line + "\n")

    out.write("\n=== Client Summary ===\n")
    for cid, d in CLIENT_SUMMARIES:
        out.write(
            f"client-{cid}: samples={int(d['samples'])} avg={d['avg']:.3f}ms "
            f"median={d['median']:.3f}ms p95={d['p95']:.3f}ms p99={d['p99']:.3f}ms "
            f"commits={int(d['commits'])} aborts={int(d['aborts'])}\n"
        )

    if CLIENT_SUMMARIES:
        out.write("\nAggregated client latency:\n")
        out.write(
            f"samples={int(agg['samples'])} avg={agg['avg']:.3f}ms "
            f"median={agg['median']:.3f}ms p95={agg['p95']:.3f}ms "
            f"p99={agg['p99']:.3f}ms commits={int(agg['commits'])} "
            f"aborts={int(agg['aborts'])}\n"
        )

print(f"\nWrote combined results to {RESULTS_FILE}")
