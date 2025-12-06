#!/usr/bin/env python3
# RUN IT LIKE THIS: python3 get_kvs_logs.py /path/to/logs_root

import sys
import re
import os
import csv
from pathlib import Path
from statistics import mean, median

#parse a kvsserver-nodeX.log file into a list of time series records
#each record is a dictionary
def parse_server_log(path):
    records= {}
    policy= None
    commits= None
    aborts= None
    ops= None
    time_idx=0
    time_limit=30
    start=False

    re_policy= re.compile(r"^(\S+)$")
    re_commits= re.compile(r"^commits/s\s+([0-9.]+)")
    re_aborts= re.compile(r"^aborts/s\s+([0-9.]+)")

    commits = []
    aborts = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if policy is None:
                m = re_policy.match(line)
                if m:
                    policy = m.group(1)
                    continue
            m = re_commits.match(line)
            if m:
                commits.append(float(m.group(1)))
                if float(m.group(1))>0 and time_idx<=time_limit:
                    start=True
                continue

            m = re_aborts.match(line)
            if m:
                aborts.append(float(m.group(1)))
                if float(m.group(1))>0 and time_idx<=time_limit:
                    start=True
                if not start:
                    commits.pop()
                    aborts.pop()
                else:
                    time_idx+=1
                
                if time_idx>time_limit:
                    break
                continue
    
    records['policy']=policy

    if commits:
        median_c = median(sorted(commits))
        records['comm_rate']=median_c
    else:
        print(f"{node}: no commits/s data")

    if aborts:
        median_a = median(sorted(aborts))
        records['abrt_rate']=median_a
    else:
        print(f"{node}: no aborts/s data")

    return records

#parse kvsclient-nodeX.log file . return a dictionaryimport re

def parse_client_meta(path):
    meta = {}

    re_hosts     = re.compile(r"^hosts\s+(.*)")
    re_theta     = re.compile(r"^theta\s+([0-9.]+)")
    re_workload  = re.compile(r"^workload\s+(.+)")
    re_secs      = re.compile(r"^secs\s+([0-9.]+)")
    re_opspertx  = re.compile(r"^opsPerTx\s+([0-9.]+)")
    re_policy    = re.compile(r"^policy\s+(.+)")
    re_threads   = re.compile(r"^threads\s+([0-9.]+)")

    # CLIENT-SUMMARY: attempts=72897 commits=62126 aborts=10771 ...
    re_summary   = re.compile(
        r"^CLIENT-SUMMARY:\s*"
        r"attempts=(\d+)\s+commits=(\d+)\s+aborts=(\d+)\s+samples=(\d+)\s+"
        r"avg_ms=([0-9.]+)\s+median_ms=([0-9.]+)\s+p95_ms=([0-9.]+)\s+p99_ms=([0-9.]+)"
    )

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            m = re_hosts.match(line)
            if m:
                meta["hosts"] = m.group(1).strip()
                continue

            m = re_theta.match(line)
            if m:
                meta["theta"] = float(m.group(1))
                continue

            m = re_workload.match(line)
            if m:
                meta["workload"] = m.group(1).strip()
                continue

            m = re_secs.match(line)
            if m:
                meta["secs"] = float(m.group(1))
                continue

            m = re_opspertx.match(line)
            if m:
                meta["opsPerTx"] = float(m.group(1))
                continue

            m = re_policy.match(line)
            if m:
                meta["policy"] = m.group(1).strip()
                continue

            m = re_threads.match(line)
            if m:
                meta["threads"] = int(m.group(1))
                continue

            m = re_summary.match(line)
            if m:
                meta["CLIENT-SUMMARY"] = {
                    "attempts" :   int(m.group(1)),
                    "commits" :    int(m.group(2)),
                    "aborts" :     int(m.group(3)),
                    "samples" :    int(m.group(4)),
                    "avg_ms" :     float(m.group(5)),
                    "median_ms" :  float(m.group(6)),
                    "p95_ms" :     float(m.group(7)),
                    "p99_ms" :     float(m.group(8)),
                }
                continue

    return meta

"""
each row has:
    - run_dir
    - node
    - role ("server")
    - time_idx
    - commits_per_s
    - aborts_per_s
    - ops_per_s
    - hosts, theta, workload, secs  (client)
"""

def collect_logs(logs_root: Path):
    rows= []
    client_pattern = "kvsclient"
    server_pattern = "kvsserver"
    for subdir, dirs, files in os.walk(logs_root):
        meta = {}
        client_recs=[]
        server_recs=[]
        policy=""
        total_threads=0

        for file in files:
            fpath = subdir + "/" + file
            if re.search(client_pattern, file) != None:
                meta = parse_client_meta(fpath)
                total_threads+=meta['threads']
                client_recs.append(meta)

            if re.search(server_pattern, file) != None:
                stem = file  # something like "kvsserver-node0"
                parts = stem.split("-")
                policy = parts[2] if len(parts) > 1 else stem
                repeat = parts[1] if len(parts) > 1 else stem
                node = parts[-1] if len(parts) > 1 else stem
                if node == "node0.log":
                    records = parse_server_log(fpath)
                    server_recs.append(records)

        total_threads /= 3
        if len(client_recs) > 0 and len(server_recs) > 0:
            final_client_rec = {}
            avg_ms = []
            median_ms = []
            p95_ms = []
            p99_ms = []
            for client_rec in client_recs:
                if "CLIENT-SUMMARY" not in client_rec:
                    continue
                avg_ms.append(client_rec["CLIENT-SUMMARY"]['avg_ms'])
                median_ms.append(client_rec["CLIENT-SUMMARY"]['median_ms'])
                p95_ms.append(client_rec["CLIENT-SUMMARY"]['p95_ms'])
                p99_ms.append(client_rec["CLIENT-SUMMARY"]['p99_ms'])
            final_client_rec = {
                'avg_ms': median(avg_ms),
                'median_ms': median(median_ms),
                'p95_ms': median(p95_ms),
                'p99_ms': median(p99_ms),
            }

            final_server_rec = {}
            comm_rate=[]
            abrt_rate=[]
            for server_rec in server_recs:
                comm_rate.append(server_rec['comm_rate'])
                abrt_rate.append(server_rec['abrt_rate'])
            final_server_rec={
                'comm_rate': median(comm_rate),
                'abrt_rate': median(abrt_rate),
            }

            row = {
                "fpath" : subdir,
                "policy" : policy[9:],
                "theta" : meta.get("theta", ""),
                "workload" : meta.get("workload", ""),
                "secs" : int(meta.get("secs", "30")),
                "opsPerTx" : int(meta.get("opsPerTx", "")),
                "total_threads" : int(total_threads),
                "comm_rate" : final_server_rec['comm_rate'],
                "abrt_rate" : final_server_rec['abrt_rate'],
                "avg_ms" : final_client_rec['avg_ms'],
                "median_ms" : final_client_rec['median_ms'],
                "p95_ms" : final_client_rec['p95_ms'],
                "p99_ms" : final_client_rec['p99_ms'],
            }
            rows.append(row)
    return rows

def write_raw_csv(rows, out_path: Path):
    if not rows:
        print("NO DATA ROWS COLLECTED. not writing kvs_raw.csv.")
        return

    fieldnames= [
        "fpath",
        "policy",
        "theta",
        "workload",
        "secs",
        "opsPerTx",
        "total_threads",
        "comm_rate",
        "abrt_rate",
        "avg_ms",
        "median_ms",
        "p95_ms",
        "p99_ms",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer= csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def main():
    if len(sys.argv) != 2:
        sys.exit(1)

    logs_root = Path(sys.argv[1]).expanduser().resolve()
    if not logs_root.is_dir():
        sys.exit(1)

    print(f"getting logs from {logs_root} ...")
    rows = collect_logs(logs_root)
    print(f"Collected {len(rows)} rows.")

    raw_path = Path("kvs_data.csv").resolve()
    write_raw_csv(rows, raw_path)
    print("done")


if __name__ == "__main__":
    main()
