#!/usr/bin/env python3

# Extracts throughputs and summarizes them from a set of kvs server logs.
# Mostly thrown together with Claude from a draft awk script.

import os
import glob
import statistics

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
        print(f"{node} median {median_val:.0f} commits/s")
        total_commits += median_val
    else:
        print(f"{node} no commits/s data found")
    if aborts:
        median_val = statistics.median(sorted(aborts))
        print(f"{node} median {median_val:.0f} aborts/s")
        total_aborts += median_val
    else:
        print(f"{node} no aborts/s data found")

print()
print(f"total {total_commits:.0f} commits/s")
print(f"total {total_aborts:.0f} aborts/s")
