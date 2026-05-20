"""Serial baseline: one process, one URL at a time.

Establishes T(1), the reference wallclock all parallel speedups are computed
against.

    python -m src.scraper_serial --urls data/urls.txt [--keyword FIS]
                                 [--work-multiplier 5] [--limit N]
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from .processing import (
    DEFAULT_KEYWORD,
    DEFAULT_WORK_MULTIPLIER,
    fetch_and_process,
    load_urls,
    summarize,
)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--urls", default="data/urls.txt")
    p.add_argument("--keyword", default=DEFAULT_KEYWORD)
    p.add_argument("--work-multiplier", type=int, default=DEFAULT_WORK_MULTIPLIER)
    p.add_argument("--limit", type=int, default=0,
                   help="Process only first N URLs (0 = all).")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    urls = load_urls(args.urls)
    if args.limit > 0:
        urls = urls[: args.limit]

    if not args.quiet:
        print(f"[serial] {len(urls)} URLs, keyword='{args.keyword}', "
              f"work_multiplier={args.work_multiplier}", file=sys.stderr)

    t0 = time.perf_counter()
    results = [
        fetch_and_process(u, args.keyword, work_multiplier=args.work_multiplier)
        for u in urls
    ]
    elapsed = time.perf_counter() - t0

    summary = summarize(results)
    summary["framework"] = "serial"
    summary["n_workers"] = 1
    summary["wall_time_s"] = elapsed

    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
