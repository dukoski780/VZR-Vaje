"""Compute speedup, efficiency and the Karp-Flatt metric from a benchmark sweep.

Reads benchmarks/results.jsonl (from run_benchmarks.sh), writes results.csv
(one row per run) and summary.csv (one row per framework/worker count), and
prints the summary table plus a content-analysis report to the console.

The serial baseline T(1) is the median of the serial runs, which is robust
against the cold first-fetch outlier. Standard library only.

    python benchmarks/analyze.py
"""

from __future__ import annotations

import csv
import json
import math
import statistics
from pathlib import Path

HERE = Path(__file__).parent
RESULTS_JSONL = HERE / "results.jsonl"
RESULTS_CSV   = HERE / "results.csv"
SUMMARY_CSV   = HERE / "summary.csv"

CONTENT_FIELDS = [
    "avg_words_per_page", "avg_unique_words_per_page", "avg_type_token_ratio",
    "n_pages_sl", "n_pages_en",
    "avg_words_sl", "avg_words_en",
    "avg_ttr_sl", "avg_ttr_en",
    "avg_page_size_bytes",
    "total_keyword_hits", "n_pages_with_keyword",
    "n_ok", "n_total",
]


def load_runs() -> list[dict]:
    rows = []
    with open(RESULTS_JSONL, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln:
                rows.append(json.loads(ln))
    return rows


def write_results_csv(runs: list[dict]) -> None:
    """One row per run; columns are the union of keys across all frameworks."""
    fieldnames: list[str] = []
    for r in runs:
        for k in r:
            if k not in fieldnames:
                fieldnames.append(k)
    with open(RESULTS_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in runs:
            w.writerow(r)


def karp_flatt(speedup: float, p: int) -> float:
    """Experimentally determined serial fraction; undefined at p=1."""
    if p <= 1 or speedup <= 0:
        return float("nan")
    return (1.0 / speedup - 1.0 / p) / (1.0 - 1.0 / p)


def build_summary(runs: list[dict]) -> list[dict]:
    """Aggregate wall times per (framework, n_workers) into the metrics table."""
    groups: dict[tuple[str, int], list[float]] = {}
    for r in runs:
        key = (r["framework"], int(r["n_workers"]))
        groups.setdefault(key, []).append(float(r["wall_time_s"]))

    serial_times = [float(r["wall_time_s"]) for r in runs
                    if r["framework"] == "serial"]
    t1_serial = statistics.median(serial_times) if serial_times else float("nan")  # robust baseline

    summary = []
    for (framework, p), times in groups.items():
        t_mean = statistics.mean(times)
        t_std = statistics.stdev(times) if len(times) > 1 else 0.0
        speedup = t1_serial / t_mean if t_mean > 0 else float("nan")
        summary.append({
            "framework": framework,
            "n_workers": p,
            "n_runs": len(times),
            "t_mean_s": t_mean,
            "t_std_s": t_std,
            "t_min_s": min(times),
            "t_max_s": max(times),
            "speedup_vs_serial": speedup,
            "efficiency": speedup / p if p > 0 else float("nan"),
            "karp_flatt_e": karp_flatt(speedup, p),
        })

    summary.sort(key=lambda row: (row["framework"], row["n_workers"]))
    return summary


def write_summary_csv(summary: list[dict]) -> None:
    fieldnames = ["framework", "n_workers", "n_runs",
                  "t_mean_s", "t_std_s", "t_min_s", "t_max_s",
                  "speedup_vs_serial", "efficiency", "karp_flatt_e"]
    with open(SUMMARY_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in summary:
            w.writerow(row)


def print_summary(summary: list[dict]) -> None:
    headers = ["Framework", "Workers", "Runs", "T mean [s]", "T std [s]",
               "Speedup", "Efficiency", "Karp-Flatt e"]
    widths = [10, 7, 4, 11, 10, 8, 10, 12]

    def fmt_row(cells: list[str]) -> str:
        return "  ".join(c.rjust(w) for c, w in zip(cells, widths))

    print("\n=== Benchmark summary (3-run mean) ===\n")
    print(fmt_row(headers))
    print("  ".join("-" * w for w in widths))
    for r in summary:
        e = r["karp_flatt_e"]
        print(fmt_row([
            r["framework"],
            str(r["n_workers"]),
            str(r["n_runs"]),
            f"{r['t_mean_s']:.3f}",
            f"{r['t_std_s']:.3f}",
            f"{r['speedup_vs_serial']:.3f}",
            f"{r['efficiency']:.3f}",
            "-" if math.isnan(e) else f"{e:.4f}",
        ]))


def print_content_stats(runs: list[dict]) -> None:
    """Mean content metrics across serial runs (deterministic for a fixed URL set)."""
    serial = [r for r in runs if r["framework"] == "serial"]
    if not serial:
        return

    def avg(field: str) -> float:
        vals = [float(r[field]) for r in serial if field in r]
        return statistics.mean(vals) if vals else 0.0

    print("\n=== Content analysis (mean over serial runs) ===\n")
    print(f"  Pages OK / total       : {avg('n_ok'):.0f} / {avg('n_total'):.0f}")
    print(f"  Slovenian / English    : {avg('n_pages_sl'):.0f} / {avg('n_pages_en'):.0f}")
    print(f"  Avg words / page       : {avg('avg_words_per_page'):.0f}")
    print(f"  Avg unique words / page: {avg('avg_unique_words_per_page'):.0f}")
    print(f"  Avg type-token ratio   : {avg('avg_type_token_ratio'):.3f}")
    print(f"  Avg words (SL / EN)    : {avg('avg_words_sl'):.0f} / {avg('avg_words_en'):.0f}")
    print(f"  Avg page size          : {avg('avg_page_size_bytes') / 1024:.0f} KB")
    print(f"  Keyword hits (pages)   : {avg('total_keyword_hits'):.0f} "
          f"({avg('n_pages_with_keyword'):.0f} pages)")


def main() -> None:
    if not RESULTS_JSONL.exists():
        raise SystemExit(f"missing {RESULTS_JSONL} - run run_benchmarks.sh first")

    runs = load_runs()
    write_results_csv(runs)

    summary = build_summary(runs)
    write_summary_csv(summary)

    print_summary(summary)
    print_content_stats(runs)

    print(f"\nWrote {RESULTS_CSV} and {SUMMARY_CSV}")


if __name__ == "__main__":
    main()
