#!/usr/bin/env python3
"""
Load test for the job scheduler. Standard library only (Python 3.8+).

Submits N jobs as fast as possible, waits for all of them to reach a terminal state, then reports throughput and
end-to-end latency computed from the SERVER's own timestamps (createdAt -> finishedAt), so client polling
granularity does not distort the result.

Each job is a "sleep" job that simulates --work-ms of work, so the ceiling you measure is
    (workers x consumers-per-worker) / work-ms
and you can watch it scale as you add workers:

    docker compose up -d --scale worker=1
    python scripts/loadtest.py --jobs 300 --work-ms 100
    docker compose up -d --scale worker=3
    python scripts/loadtest.py --jobs 300 --work-ms 100
    docker compose up -d --scale worker=6
    python scripts/loadtest.py --jobs 300 --work-ms 100

Note: latency here includes time spent waiting in the queue, so a burst larger than capacity shows rising latency.
That is expected; throughput is the scaling number, latency shows queueing under that load.
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

TERMINAL = ("SUCCEEDED", "DEAD")


def call(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())


def parse_ts(value):
    """Parses '2026-10-22T20:13:38.194239107Z' (any fractional precision) to a naive UTC datetime."""
    value = value.rstrip("Z")
    if "." in value:
        head, fraction = value.split(".")
        value = f"{head}.{fraction[:6]:0<6}"
    return datetime.fromisoformat(value)


def percentile(sorted_values, p):
    if not sorted_values:
        return float("nan")
    return sorted_values[int(p * (len(sorted_values) - 1))]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api", default="http://localhost:8081", help="API base URL (default: %(default)s)")
    parser.add_argument("--jobs", type=int, default=300, help="number of jobs to submit (default: %(default)s)")
    parser.add_argument("--work-ms", type=int, default=100, help="simulated work per job (default: %(default)s)")
    parser.add_argument("--submit-threads", type=int, default=16, help="parallel submitters (default: %(default)s)")
    parser.add_argument("--timeout", type=int, default=600, help="give up after this many seconds")
    args = parser.parse_args()

    payload = {"type": "sleep", "payload": {"millis": args.work_ms}, "maxAttempts": 1}

    print(f"Submitting {args.jobs} jobs ({args.work_ms} ms of work each) to {args.api} ...")
    submit_start = time.time()
    try:
        with ThreadPoolExecutor(args.submit_threads) as pool:
            ids = list(pool.map(lambda _: call("POST", f"{args.api}/api/jobs", payload)["id"], range(args.jobs)))
    except (urllib.error.URLError, OSError) as error:
        sys.exit(f"Could not reach the API at {args.api}: {error}")
    submit_seconds = time.time() - submit_start
    print(f"  accepted {len(ids)} jobs in {submit_seconds:.2f}s ({len(ids) / submit_seconds:.0f} submissions/s)")

    print("Waiting for jobs to finish ...")
    done = {}
    deadline = time.time() + args.timeout
    with ThreadPoolExecutor(args.submit_threads) as pool:
        while len(done) < len(ids) and time.time() < deadline:
            pending = [job_id for job_id in ids if job_id not in done]
            for job in pool.map(lambda job_id: call("GET", f"{args.api}/api/jobs/{job_id}"), pending):
                if job["status"] in TERMINAL:
                    done[job["id"]] = job
            print(f"  {len(done)}/{len(ids)} finished", end="\r")
            if len(done) < len(ids):
                time.sleep(0.5)
    print()

    jobs = list(done.values())
    succeeded = [j for j in jobs if j["status"] == "SUCCEEDED"]
    dead = [j for j in jobs if j["status"] == "DEAD"]
    if not succeeded:
        sys.exit("No job succeeded; nothing to report.")

    created = [parse_ts(j["createdAt"]) for j in succeeded]
    finished = [parse_ts(j["finishedAt"]) for j in succeeded]
    wall = (max(finished) - min(created)).total_seconds()
    latencies_ms = sorted((f - c).total_seconds() * 1000 for c, f in zip(created, finished))

    print()
    print("================ RESULT ================")
    print(f"jobs submitted        : {len(ids)}")
    print(f"succeeded / dead / lost: {len(succeeded)} / {len(dead)} / {len(ids) - len(jobs)}")
    print(f"simulated work per job : {args.work_ms} ms")
    print(f"wall time (first create -> last finish): {wall:.2f} s")
    print(f"THROUGHPUT             : {len(succeeded) / wall:.1f} jobs/s")
    print(f"latency p50 / p95 / p99: {percentile(latencies_ms, 0.50):.0f} / "
          f"{percentile(latencies_ms, 0.95):.0f} / {percentile(latencies_ms, 0.99):.0f} ms (queue wait + work)")
    print("========================================")
    if len(jobs) < len(ids):
        print(f"WARNING: {len(ids) - len(jobs)} job(s) did not finish within {args.timeout}s", file=sys.stderr)


if __name__ == "__main__":
    main()
