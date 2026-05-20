"""Manager-Worker web scraper using mpi4py.

Rank 0 is the manager: it holds the URL queue, hands one URL to each worker,
then on each returned result immediately dispatches the next URL to whoever
just reported back (dynamic load balancing). A SENTINEL tells each worker to
exit. Ranks > 0 are workers: recv URL -> fetch_and_process -> send result.

This "bag of tasks" pattern (Quinn ch. 8) is the right choice when task sizes
vary, as web pages do — a page may be 500 bytes or 500 KB and fetch latency
ranges from milliseconds to seconds.

    mpiexec -n <P> python -m src.scraper_mpi --urls data/urls.txt \
        [--keyword FIS] [--work-multiplier 5] [--limit N]

P must be >= 2 (one manager + >= 1 worker). Reported workers is P-1 — the
value to use as `p` in speedup / Karp-Flatt. For p=1 use scraper_serial.
"""

from __future__ import annotations

import argparse
import json
import sys

from mpi4py import MPI

from .processing import (
    DEFAULT_KEYWORD,
    DEFAULT_WORK_MULTIPLIER,
    PageResult,
    fetch_and_process,
    load_urls,
    summarize,
)

TAG_TASK = 1
TAG_RESULT = 2
SENTINEL = None


def manager(comm: MPI.Comm, urls: list[str], quiet: bool) -> list[PageResult]:
    size = comm.Get_size()
    n_workers = size - 1
    if not quiet:
        print(f"[mpi/manager] dispatching {len(urls)} URLs to {n_workers} workers",
              file=sys.stderr)

    results: list[PageResult] = []
    url_iter = iter(urls)
    in_flight = 0

    # Prime each worker with one task.
    for w in range(1, size):
        try:
            u = next(url_iter)
            comm.send(u, dest=w, tag=TAG_TASK)
            in_flight += 1
        except StopIteration:
            comm.send(SENTINEL, dest=w, tag=TAG_TASK)

    # Receive results, dispatch the next URL to the worker that just
    # reported back. This is the dynamic load-balancing step.
    status = MPI.Status()
    while in_flight > 0:
        result: PageResult = comm.recv(
            source=MPI.ANY_SOURCE, tag=TAG_RESULT, status=status
        )
        results.append(result)
        in_flight -= 1
        src = status.Get_source()
        try:
            u = next(url_iter)
            comm.send(u, dest=src, tag=TAG_TASK)
            in_flight += 1
        except StopIteration:
            comm.send(SENTINEL, dest=src, tag=TAG_TASK)

    return results


def worker(comm: MPI.Comm, keyword: str, work_multiplier: int) -> None:
    while True:
        task = comm.recv(source=0, tag=TAG_TASK)
        if task is SENTINEL:
            return
        url: str = task
        result = fetch_and_process(url, keyword, work_multiplier=work_multiplier)
        comm.send(result, dest=0, tag=TAG_RESULT)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--urls", default="data/urls.txt")
    p.add_argument("--keyword", default=DEFAULT_KEYWORD)
    p.add_argument("--work-multiplier", type=int, default=DEFAULT_WORK_MULTIPLIER)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if size < 2:
        if rank == 0:
            print("scraper_mpi requires at least 2 processes "
                  "(1 manager + >=1 worker). Use scraper_serial for p=1.",
                  file=sys.stderr)
        return 2

    urls = load_urls(args.urls)
    if args.limit > 0:
        urls = urls[: args.limit]

    # Time only the work phase, not interpreter / MPI init.
    comm.Barrier()
    t0 = MPI.Wtime()

    if rank == 0:
        results = manager(comm, urls, args.quiet)
    else:
        worker(comm, args.keyword, args.work_multiplier)
        results = None  # type: ignore[assignment]

    comm.Barrier()
    elapsed = MPI.Wtime() - t0

    if rank == 0:
        summary = summarize(results)
        summary["framework"] = "mpi"
        summary["n_processes"] = size
        summary["n_workers"] = size - 1
        summary["wall_time_s"] = elapsed
        print(json.dumps(summary, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())
