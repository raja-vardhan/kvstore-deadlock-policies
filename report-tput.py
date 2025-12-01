#!/usr/bin/env python3

# Extracts throughputs and summarizes them from a set of kvs server logs.
# Mostly thrown together with Claude from a draft awk script.

import os
import glob
import statistics
import re
import math

LOG_DIR = "./logs/latest"

total_commits = 0.0
total_aborts = 0.0

# Find all matching log files
log_files = sorted(glob.glob(os.path.join(LOG_DIR, "kvsserver-*.log")))

if not log_files:
    print("No matching log files found.")
    exit(0)

for log_path in log_files:
    node = os.path.basename(log_path).removeprefix("kvsserver-").removesuffix(".log")
    commits = []
    aborts = []
    with open(log_path) as f:
        for line in f:
            if "commits/s" in line:
                parts = line.strip().split()
                try:
                    # Throughput value is the 2nd column in original awk ($2)
                    commits.append(float(parts[1]))
                except (IndexError, ValueError):
                    pass
            elif "aborts/s" in line:
                parts = line.strip().split()
                try:
                    # Throughput value is the 2nd column in original awk ($2)
                    aborts.append(float(parts[1]))
                except (IndexError, ValueError):
                    pass
    if commits:
        median_val = statistics.median(sorted(commits))
        mean_val = statistics.mean(commits)
        peak_val = max(commits)
        print(f"{node} mean {mean_val:.0f} commits/s")
        print(f"{node} median {median_val:.0f} commits/s")
        print(f"{node} peak {peak_val:.0f} commits/s")
        total_commits += median_val
    else:
        print(f"{node} no commits/s data found")
    if aborts:
        median_val = statistics.median(sorted(aborts))
        mean_val = statistics.mean(aborts)
        peak_val = max(aborts)
        print(f"{node} mean {mean_val:.0f} aborts/s")
        print(f"{node} median {median_val:.0f} aborts/s")
        print(f"{node} median {peak_val:.0f} aborts/s")
        total_aborts += median_val
    else:
        print(f"{node} no aborts/s data found")

print()
print(f"total {total_commits:.0f} commits/s")
print(f"total {total_aborts:.0f} aborts/s")

#Client logs
CLIENT_SAMPLE_RE = re.compile(r"client-(?P<id>\d+)\s+latency_sample:\s*(?P<val>[-+]?\d*\.?\d+)\s*ms", re.IGNORECASE)

def percentile(sorted_data, p: float):
    """Return pth percentile (linear interpolation) from a non-empty sorted list."""
    if not sorted_data:
        raise ValueError("percentile requires non-empty data")
    if p <= 0:
        return sorted_data[0]
    if p >= 100:
        return sorted_data[-1]
    n = len(sorted_data)
    pos = (p / 100.0) * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_data[lo]
    weight = pos - lo
    return sorted_data[lo] * (1 - weight) + sorted_data[hi] * weight

client_log_files = sorted(glob.glob(os.path.join(LOG_DIR, "client-*.log")))

if not client_log_files:
    print("\nNo matching client log files found (no raw latency samples).")
else:
    print("\nParsing client raw latency samples...")
    client_samples = {}

    for log_path in client_log_files:
        name = os.path.basename(log_path)
        mfile = re.match(r"client-(\d+)\.log$", name)
        default_id = mfile.group(1) if mfile else None

        with open(log_path) as f:
            for line in f:
                m = CLIENT_SAMPLE_RE.search(line)
                if m:
                    cid = m.group("id")
                    try:
                        val = float(m.group("val"))
                    except ValueError:
                        continue
                    client_samples.setdefault(cid, []).append(val)
                else:
                    if default_id:
                        m2 = re.search(r"latency_sample:\s*([-+]?\d*\.?\d+)\s*ms", line, re.IGNORECASE)
                        if m2:
                            try:
                                val = float(m2.group(1))
                                client_samples.setdefault(default_id, []).append(val)
                            except ValueError:
                                pass

    # Compute per-client stats and aggregate
    all_samples = []
    if not client_samples:
        print("\nNo raw latency samples found in client logs.")
    else:
        print()
        for cid in sorted(client_samples, key=lambda x: int(x)):
            samples = client_samples[cid]
            if not samples:
                print(f"client-{cid}: no samples")
                continue
            samples_sorted = sorted(samples)
            cnt = len(samples_sorted)
            mean_val = statistics.mean(samples_sorted)
            median_val = statistics.median(samples_sorted)
            p95 = percentile(samples_sorted, 95.0)
            p99 = percentile(samples_sorted, 99.0)
            mn = samples_sorted[0]
            mx = samples_sorted[-1]
            all_samples.extend(samples_sorted)
            print(f"client-{cid} samples={cnt} mean={mean_val:.3f}ms median={median_val:.3f}ms p95={p95:.3f}ms p99={p99:.3f}ms min={mn:.3f}ms max={mx:.3f}ms")

        # aggregated across clients
        if all_samples:
            all_sorted = sorted(all_samples)
            cnt = len(all_sorted)
            mean_all = statistics.mean(all_sorted)
            median_all = statistics.median(all_sorted)
            p95_all = percentile(all_sorted, 95.0)
            p99_all = percentile(all_sorted, 99.0)
            mn = all_sorted[0]
            mx = all_sorted[-1]
            print()
            print("Aggregated across all clients:")
            print(f"samples={cnt} mean={mean_all:.3f}ms median={median_all:.3f}ms p95={p95_all:.3f}ms p99={p99_all:.3f}ms min={mn:.3f}ms max={mx:.3f}ms")
