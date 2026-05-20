"""PySpark web scraper.

Spark's task scheduler is itself the manager-worker: we hand it the URL list
as an RDD and map `fetch_and_process` over it. The driver (this JVM) is the
manager, executor threads are the workers, and master=local[N] gives N worker
threads in one JVM — a direct analogue of the MPI version's N worker processes.
The same `fetch_and_process` is used as in the other runners, which is what
keeps the comparison fair.

Timing: SparkContext start-up costs ~5-15 s, so the timer starts only after
the context exists; reported wall_time_s covers just parallelize + map +
collect. See README.md for how this is treated in the analysis.

    python -m src.scraper_spark --workers 4 --urls data/urls.txt \
        [--keyword FIS] [--work-multiplier 5] [--limit N]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

# Quiet Spark down before SparkContext starts.
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 1g pyspark-shell")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
# Force Spark Python executors to use the same interpreter as the driver
# so they pick up the venv with bs4 / requests installed. Without this
# Spark falls back to the system python which lacks our dependencies.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark import SparkConf, SparkContext  # noqa: E402

from .processing import (
    DEFAULT_KEYWORD,
    DEFAULT_WORK_MULTIPLIER,
    fetch_and_process,
    load_urls,
    summarize,
)

# Spark over-partitioning factor: hand the scheduler more tasks than
# threads so straggler URLs don't stall a worker for the whole stage.
PARTITIONS_PER_WORKER = 4


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, required=True,
                   help="Number of local Spark worker threads (local[N]).")
    p.add_argument("--urls", default="data/urls.txt")
    p.add_argument("--keyword", default=DEFAULT_KEYWORD)
    p.add_argument("--work-multiplier", type=int, default=DEFAULT_WORK_MULTIPLIER)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    urls = load_urls(args.urls)
    if args.limit > 0:
        urls = urls[: args.limit]

    n_partitions = min(PARTITIONS_PER_WORKER * args.workers, len(urls))

    conf = (
        SparkConf()
        .setAppName("vzr-web-scraper")
        .setMaster(f"local[{args.workers}]")
        .set("spark.ui.showConsoleProgress", "false")
        .set("spark.log.level", "ERROR")
    )
    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("ERROR")

    if not args.quiet:
        print(f"[spark] master=local[{args.workers}] "
              f"partitions={n_partitions} urls={len(urls)}",
              file=sys.stderr)

    keyword = args.keyword
    work_multiplier = args.work_multiplier

    def _run(url: str):
        return fetch_and_process(url, keyword, work_multiplier=work_multiplier)

    try:
        t0 = time.perf_counter()
        rdd = sc.parallelize(urls, numSlices=n_partitions)
        results = rdd.map(_run).collect()
        elapsed = time.perf_counter() - t0
    finally:
        sc.stop()

    summary = summarize(results)
    summary["framework"] = "spark"
    summary["n_workers"] = args.workers
    summary["n_partitions"] = n_partitions
    summary["wall_time_s"] = elapsed
    print(json.dumps(summary, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())
