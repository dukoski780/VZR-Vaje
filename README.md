# Simulator of a Simple Web Crawler — Parallel Processing with mpi4py and Apache Spark

Seminar project for **Visoko-zmogljivo računalništvo** (High-Performance
Computing). A parallel web scraper fetches all pages of the **FIS UNM website**
(`fis.unm.si`) — discovered automatically from the WordPress sitemap — and runs
a CPU-intensive text-processing pipeline on each page, using two parallel
programming models:

* **mpi4py** with an explicit Manager-Worker protocol
* **Apache Spark (PySpark)** in `local[N]` mode (N worker threads in one JVM,
  no cluster)

Both call the **same** per-page work function (`fetch_and_process`), so the
speedup / efficiency / Karp-Flatt comparison measures only the parallelisation
strategy, never the per-page workload.

---

## Problem and solution

**Problem.** Crawling and analysing a whole website is an embarrassingly
parallel but *load-imbalanced* workload: pages differ 100× in size and fetch
time, so naive static work-splitting leaves most workers idle waiting for the
slowest one. The question this project answers: how do we parallelise it
efficiently, and which runtime — explicit message passing (MPI) or a dataflow
scheduler (Spark) — does it better on a single machine?

**Solution.** A dynamic Manager-Worker scheduler that hands the next URL to
whichever worker just finished, implemented twice — once in mpi4py, once in
PySpark — over a shared CPU-bound per-page pipeline. Performance is measured at
1–16 workers (3 runs each) and characterised with speedup, efficiency and the
Karp-Flatt metric to pinpoint the bottleneck.

> **Two commands to reproduce the results below**
> ```bash
> bash benchmarks/run_benchmarks.sh --workers "1 2 4 8 16" --repeats 3
> python3 benchmarks/analyze.py
> ```
> `--workers` selects how many cores/workers to sweep over. `analyze.py` needs
> only the Python standard library and prints the speedup / efficiency /
> Karp-Flatt table to the console.

---

## Repository layout

```
vzr/
├── requirements.txt           Python dependencies
├── data/
│   ├── build_urls.py          fetches FIS sitemap → generates data/urls.txt
│   └── urls.txt               2 577 FIS URLs (1 506 SL + 1 071 EN)
├── src/
│   ├── processing.py          fetch_and_process — shared CPU pipeline
│   ├── scraper_serial.py      1-process baseline (defines T(1))
│   ├── scraper_mpi.py         mpi4py Manager-Worker
│   └── scraper_spark.py       PySpark local[N]
└── benchmarks/
    ├── run_benchmarks.sh      full sweep launcher (serial + MPI + Spark)
    └── analyze.py             reads results.jsonl → console table + CSV (stdlib only)
```

---

## Data source — fis.unm.si

URLs are discovered automatically at runtime from the WordPress sitemap
(`https://www.fis.unm.si/sitemap_index.xml`). The site uses **WPML**, so the
sitemap includes both language versions:

| Language | URLs | Example |
|---|---|---|
| Slovenian (default) | **1 506** | `https://www.fis.unm.si/zimska-sola-2010/` |
| English (`?lang=en`) | **1 071** | `https://www.fis.unm.si/zimska-sola-2010/?lang=en` |
| **Total** | **2 577** | |

Regenerate the list at any time (`-v` for per-sub-sitemap counts):

```bash
python data/build_urls.py
```

---

## Quick start

```bash
# 1. system packages (one-time)
sudo apt-get install -y openjdk-17-jre-headless python3-pip python3-venv
sudo apt-get install -y openmpi-bin libopenmpi-dev   # or: mpich

# 2. python env
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 3. build URL list from FIS sitemap (requires internet)
.venv/bin/python data/build_urls.py

# 4. quick smoke test (5 pages)
.venv/bin/python -m src.scraper_serial --limit 5 --work-multiplier 1
mpiexec -n 3 .venv/bin/python -m src.scraper_mpi --limit 5 --work-multiplier 1
.venv/bin/python -m src.scraper_spark --workers 2 --limit 5 --work-multiplier 1

# 5. full benchmark sweep + analysis
bash benchmarks/run_benchmarks.sh --work-multiplier 10 --repeats 3 --workers "1 2 4 8 16"
python3 benchmarks/analyze.py    # prints the results table to the console
```

> **Java is required** for the PySpark runner (the Spark driver runs on the
> JVM). The serial and MPI runners work without it. Install OpenJDK 17:
>
> ```bash
> # Debian / Ubuntu / Pop!_OS
> sudo apt-get install -y openjdk-17-jre-headless
> # Fedora / RHEL
> sudo dnf install -y java-17-openjdk-headless
> # Arch
> sudo pacman -S jre17-openjdk-headless
> # macOS (Homebrew)
> brew install openjdk@17
> ```
>
> On Linux, if `java` is not on your `PATH` but is installed under
> `/usr/lib/jvm`, `run_benchmarks.sh` detects and exports `JAVA_HOME`
> automatically. On macOS, make sure `java` is on your `PATH` (Homebrew prints
> the `export PATH=...` line to add after `brew install openjdk@17`).

> **Runtime.** The full sweep — 33 timed runs (serial + MPI×5 + Spark×5, three
> repeats each) over all 2 577 pages — takes **about 2.5 hours** on the
> development machine. Most of that is the network, not the CPU: the first
> serial pass fetches every page cold (~40 min) and every later run still
> issues 2 577 HTTP requests. The `work_multiplier` then amplifies the per-page
> compute so the parallel speedup is measurable on top of that fetch time.
>
> For a quick check, add `--limit 100` to process only the first 100 URLs (a
> few minutes). The trade-off: on a small page set the fixed overheads — MPI
> manager dispatch and Spark's JVM/Py4J start-up — become a larger share of the
> wallclock, so efficiency and Karp-Flatt `e` look worse than on the full
> dataset. Use `--limit` to confirm the pipeline runs, not to draw conclusions
> about scaling.

---

## Technologies

| Layer | Technology | Role |
|---|---|---|
| Language | **Python 3.10+** | Implementation language for every runner. |
| HTTP client | **`requests`** | Synchronous HTTP/HTTPS — each worker blocks on its own request so wallclock honestly includes network time. |
| HTML parser | **`beautifulsoup4` + `lxml`** | Strip markup so we can tokenise plain text. |
| Parallel runtime A | **`mpi4py`** on **OpenMPI** | Manager-Worker via explicit message passing. Launched with `mpiexec -n P`. |
| Parallel runtime B | **`pyspark`** (Apache Spark, `local[N]`) | Manager-Worker via Spark's task scheduler — driver JVM is the manager, executor threads are the workers. |
| Java runtime | **OpenJDK 17** | Required by Spark (driver on the JVM; Python executors via Py4J). |
| Analysis | **Python standard library** | `analyze.py` computes mean/std, speedup, efficiency, Karp-Flatt — no pandas/matplotlib. |

Worker count is exposed via the same idea in both runtimes (`-n P` for MPI,
`--workers N` for Spark). An async client (`aiohttp`) is deliberately avoided —
it would fake concurrency inside one process and defeat the runtime comparison.

---

## Architecture — Manager-Worker

Manager-Worker is **mandatory** here because of the load imbalance described
above: a static partition (BLOCK_LOW/HIGH as in `sieve.py` / `goldbach.py`)
would leave fast workers idle while one worker is stuck on the largest page. A
dynamic scheduler hands out URLs on demand instead.

```
        URL queue (data/urls.txt — 2 577 entries)
              │
              │  one URL at a time
              ▼
     ┌────────────────┐
     │    MANAGER     │   rank 0  (MPI)  /  Spark driver  (JVM)
     └──┬──────┬──────┘
        │      │  dynamic dispatch: whoever finishes first gets next URL
        ▼      ▼
    ┌──────┐ ┌──────┐  ┌──────┐
    │  W1  │ │  W2  │  │  Wn  │   ranks 1..N-1 (MPI) / executor threads (Spark)
    └──┬───┘ └──┬───┘  └──┬───┘
       │        │          │
       └────────┴──────────┘
                │
         fetch_and_process(url)  ← same function for all three runners
                │
         PageResult → manager aggregates → JSON summary
```

In MPI the key line is `comm.recv(source=MPI.ANY_SOURCE)` — the manager waits
for **whichever worker finishes first**, then immediately sends it the next URL.
That is dynamic load balancing. In Spark the RDD scheduler performs the same
role internally; we just call `rdd.map(fetch_and_process).collect()`.

---

## Per-page processing pipeline (`src/processing.py`)

```
HTTP GET (requests, 15 s timeout)
    │
    ├─ page_size_bytes  ←  len(response.content)
    ├─ language         ←  "en" if ?lang=en in URL, else "sl"
    │
    ▼
HTML → plain text  (BeautifulSoup + lxml, strips <script>/<style>)
    │
    ▼
Tokenise  (regex [A-Za-zČŠŽĆĐčšžćđ]+, keeps Slovenian letters)
    │
    ├─ n_words              total word count
    ├─ n_unique_words        vocabulary size  (Counter keys)
    ├─ type_token_ratio      n_unique / n_words  (lexical diversity)
    ├─ n_sentences           sentence boundary count
    ├─ avg_word_len          mean character length per word
    ├─ flesch                Flesch reading-ease score
    └─ keyword_hits          occurrences of keyword (default: "FIS")
    │
    └─ repeated work_multiplier times  (default K=10)
```

The `work_multiplier` is critical: at K=1 the HTTP fetch is ~70% of wallclock
and the CPU work is invisible to a speedup measurement; at K=10 the CPU
dominates (per-page CPU ≈ 2.5× the warm fetch time) and speedup is observable.
It is held constant across all frameworks so the CPU/network ratio never
changes between configurations.

---

## How to set 1 / 2 / 4 / 8 workers

One **worker** = one process/thread doing fetch + CPU work.

| Workers | Serial | MPI (`-n` = workers + 1 manager) | Spark |
|---:|---|---|---|
| 1 | `python -m src.scraper_serial` | `mpiexec -n 2 python -m src.scraper_mpi` | `--workers 1` |
| 2 | n/a | `mpiexec -n 3 python -m src.scraper_mpi` | `--workers 2` |
| 4 | n/a | `mpiexec -n 5 python -m src.scraper_mpi` | `--workers 4` |
| 8 | n/a | `mpiexec -n 9 python -m src.scraper_mpi` | `--workers 8` |
| 16 | n/a | `mpiexec -n 17 python -m src.scraper_mpi` | `--workers 16` |

> The `+1` in MPI's `-n` is the manager (rank 0); only ranks ≥ 1 do real work,
> so `workers = -n − 1`.

---

## Workload and methodology

* **Input**: all 2 577 URLs from `data/urls.txt` (1 506 Slovenian + 1 071
  English `?lang=en` variants). Fixed list, fixed order, identical across runs.
  The CPU code path is fully deterministic; only network latency is stochastic.
* **Per-page work**: the shared `fetch_and_process` pipeline (see above), with
  the CPU stage repeated K=10 times.
* **Repetitions**: every (framework, n_workers) configuration runs **3 times**;
  mean and standard deviation are reported, per the brief's requirement to use
  the average of three consecutive runs.
* **Hardware**: AMD Ryzen 9 9950X, 16 physical / 32 logical cores, Wi-Fi.
  p=16 uses all physical cores; p=32 would be oversubscription.
* **Caching**: the first serial run fetches all 2 577 pages cold (~40 min,
  network-dominated). The FIS CDN warms after that pass; subsequent runs see
  cached responses (~7 min each, CPU-dominated). All framework comparisons are
  made under the same warm-cache, CPU-bound conditions, which is why `T(1)` uses
  the *median* serial time (~435 s), robust against the cold first run. (This is
  why the serial row's T std is huge — it mixes the one cold run with two warm
  ones; only the median feeds the speedup baseline.)

---

## Results

> Run `bash benchmarks/run_benchmarks.sh` then `python3 benchmarks/analyze.py`
> to regenerate the figures below.

### Content analysis (all 2 577 pages)

| Metric | Value |
|---|---|
| Pages processed | 2 577 (0 failed) |
| Slovenian / English pages | 1 506 (58 %) / 1 071 (42 %) |
| Avg words / page | 1529 |
| Avg unique words / page | 447 |
| Avg type-token ratio | 0.292 |
| Avg words — SL / EN | 1498 / 1573 |
| Avg page size | 215 KB |
| "FIS" keyword hits | 17513 (2577 pages) |

### Benchmark summary (3-run mean)

| Framework | Workers | Runs | T mean [s] | T std [s] | Speedup | Efficiency | Karp-Flatt e |
|---|---|---|---|---|---|---|---|
| mpi | 1 | 3 | 434.700 | 1.479 | 1.000 | 1.000 | - |
| mpi | 2 | 3 | 222.815 | 0.538 | 1.951 | 0.976 | 0.0249 |
| mpi | 4 | 3 | 114.958 | 0.086 | 3.782 | 0.946 | 0.0192 |
| mpi | 8 | 3 | 61.762 | 0.126 | 7.040 | 0.880 | 0.0195 |
| mpi | 16 | 3 | 36.283 | 0.360 | 11.983 | 0.749 | 0.0223 |
| serial | 1 | 3 | 1158.061 | 1253.814 | 0.375 | 0.375 | - |
| spark | 1 | 3 | 433.087 | 1.264 | 1.004 | 1.004 | - |
| spark | 2 | 3 | 222.390 | 0.992 | 1.955 | 0.978 | 0.0230 |
| spark | 4 | 3 | 117.461 | 0.176 | 3.702 | 0.925 | 0.0269 |
| spark | 8 | 3 | 66.157 | 0.483 | 6.572 | 0.822 | 0.0310 |
| spark | 16 | 3 | 40.288 | 0.840 | 10.792 | 0.675 | 0.0322 |

---

## Interpretation (Karp-Flatt analysis)

| Formula | Definition | Meaning |
|---|---|---|
| Speedup | `S(p) = T(1) / T(p)` | How many times faster than the serial baseline |
| Efficiency | `E(p) = S(p) / p` | Fraction of ideal speedup; 1.0 = perfect |
| Karp-Flatt | `e = (1/S − 1/p) / (1 − 1/p)` | Experimentally observed serial fraction |

Reading `e`: small and flat ⇒ workload scales cleanly (the gap from ideal is
honest sequential code, e.g. the manager's dispatch loop); growing with `p` ⇒
overhead grows with worker count (manager contention or communication).

**MPI** scales cleanly across the whole range: S=1.95× at p=2, 3.78× at p=4,
7.04× at p=8, and **11.98× at p=16** (efficiency 0.75). Karp-Flatt `e` stays
flat and low at every worker count (0.025 → 0.019 → 0.020 → 0.022), which means
the gap from ideal speedup is a small, roughly constant serial fraction — the
manager's dispatch loop — and **not** an overhead that grows with worker count.
Because the workload is genuinely CPU-bound (per-page CPU ≈ 2.6× the warm fetch
time), each worker has enough compute to stay busy and there is no dispatch
starvation, even at p=16.

**Spark** tracks the same curve but sits slightly behind MPI at every `p`
(S=10.79×, E=0.68 at p=16). The qualitative difference is in `e`: where MPI's is
flat, Spark's `e` *grows* with `p` (0.023 → 0.027 → 0.031 → 0.032). That is the
signature of the fixed JVM/Py4J per-task serialisation cost — it is paid on
every task and does not shrink as workers are added, so the effective serial
fraction creeps up. Spark p=1 ≈ the serial baseline (S≈1.00), expected for a
CPU-dominated workload.

**MPI is consistently faster than Spark** at every measured `p` (p=8: 61.8 s vs.
66.2 s; p=16: 36.3 s vs. 40.3 s), and the gap widens with worker count, exactly
as the rising Spark `e` predicts. For this single-machine, CPU-bound workload
mpi4py wins on both absolute wallclock and scaling efficiency. Spark's real
advantages — fault tolerance, data-locality scheduling, and cluster scale-out —
are not exercised by a single-node benchmark.

Two effects hold the p=16 efficiency below 1.0 for both runtimes: the machine
has 16 *physical* cores, so MPI's 17 processes (16 workers + manager) and
Spark's 16 threads + driver lightly oversubscribe the cores, and the residual
~5 % serial fetch time per page (warm) is not perfectly overlappable. E≈0.7–0.75
at full core count is a healthy result.

---

## Notes

**AI use:** During the development of the content and the preparation of the
documentation, the AI tool Claude (Anthropic) was used as an aid for generating
ideas, optimising code, and drafting text. All final solutions were reviewed,
verified, and adjusted as needed by the project author.
