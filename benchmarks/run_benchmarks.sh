#!/usr/bin/env bash
#
# Full benchmark sweep: serial + MPI + Spark across multiple worker counts,
# repeated per configuration. Appends one JSON line per run to
# benchmarks/results.jsonl, which analyze.py reads to build the metrics table.
#
# Usage:
#   bash benchmarks/run_benchmarks.sh \
#       [--limit N] [--work-multiplier M] [--repeats R] [--workers "1 2 4 8 16"]
#
# Defaults: all URLs, work_multiplier=10, 3 repeats, workers="1 2 4 8 16".
# K=10 keeps the workload CPU-bound (per-page CPU ~2.5x the warm fetch time).

set -euo pipefail

# ---------- defaults ----------
LIMIT=0   # 0 = all URLs
WORK_MULTIPLIER=10
REPEATS=3
WORKERS_LIST="1 2 4 8 16"
KEYWORD="FIS"
URLS="data/urls.txt"
# ------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --limit)            LIMIT="$2"; shift 2 ;;
        --work-multiplier)  WORK_MULTIPLIER="$2"; shift 2 ;;
        --repeats)          REPEATS="$2"; shift 2 ;;
        --workers)          WORKERS_LIST="$2"; shift 2 ;;
        --keyword)          KEYWORD="$2"; shift 2 ;;
        --urls)             URLS="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Ensure Java is on the PATH for PySpark: try distro helpers, then scan JVM dirs.
if ! command -v java &>/dev/null; then
    JAVA_CANDIDATE=""
    if command -v update-alternatives &>/dev/null; then
        JAVA_CANDIDATE="$(update-alternatives --list java 2>/dev/null | head -1)"
    fi
    if [[ -z "$JAVA_CANDIDATE" ]] && command -v alternatives &>/dev/null; then
        JAVA_CANDIDATE="$(alternatives --list 2>/dev/null | awk '/^java/{print $NF}' | head -1)"
    fi
    if [[ -z "$JAVA_CANDIDATE" ]]; then
        for _jvmdir in /usr/lib/jvm /usr/local/lib/jvm /opt/java /opt/jdk /usr/java; do
            if [[ -d "$_jvmdir" ]]; then
                JAVA_CANDIDATE="$(find "$_jvmdir" -maxdepth 4 -name java -type f 2>/dev/null | head -1)"
                [[ -n "$JAVA_CANDIDATE" ]] && break
            fi
        done
    fi
    if [[ -n "$JAVA_CANDIDATE" ]]; then
        export JAVA_HOME
        JAVA_HOME="$(dirname "$(dirname "$JAVA_CANDIDATE")")"
        export PATH="$JAVA_HOME/bin:$PATH"
        echo "Java found: $JAVA_CANDIDATE"
    else
        echo "WARNING: Java not found on PATH. PySpark runs will likely fail." >&2
        echo "  Ubuntu/Debian : sudo apt-get install -y openjdk-17-jre-headless" >&2
        echo "  Fedora/RHEL   : sudo dnf install -y java-17-openjdk-headless" >&2
        echo "  Arch          : sudo pacman -S jre17-openjdk-headless" >&2
    fi
fi

# Detect MPI launcher (some distros ship only mpirun, not mpiexec).
if command -v mpiexec &>/dev/null; then
    MPIEXEC="mpiexec"
elif command -v mpirun &>/dev/null; then
    MPIEXEC="mpirun"
else
    echo "ERROR: neither mpiexec nor mpirun found." >&2
    echo "  Ubuntu/Debian : sudo apt-get install -y openmpi-bin libopenmpi-dev" >&2
    echo "  Fedora/RHEL   : sudo dnf install -y openmpi openmpi-devel" >&2
    echo "  Arch          : sudo pacman -S openmpi" >&2
    exit 1
fi
echo "Using MPI launcher: $MPIEXEC"

if [[ -x "$ROOT/.venv/bin/python" ]]; then
    PY="$ROOT/.venv/bin/python"
elif command -v python3 &>/dev/null; then
    PY="$(command -v python3)"
else
    echo "No python3 found. Create .venv or install python3." >&2
    exit 1
fi
echo "Using Python: $PY"

OUT="$ROOT/benchmarks/results.jsonl"
mkdir -p "$ROOT/benchmarks"

# Never silently clobber a prior sweep: archive any existing outputs to a
# timestamped folder before starting a fresh run.
if [[ -s "$OUT" ]]; then
    STAMP="$(date +%Y%m%d-%H%M%S)"
    ARCHIVE="$ROOT/benchmarks/archive/$STAMP"
    mkdir -p "$ARCHIVE"
    for f in results.jsonl results.csv summary.csv summary.md content_stats.json sweep.log; do
        [[ -e "$ROOT/benchmarks/$f" ]] && cp -p "$ROOT/benchmarks/$f" "$ARCHIVE/" || true
    done
    echo "Archived previous results to $ARCHIVE"
fi
: > "$OUT"
: > "$ROOT/benchmarks/sweep.log"

echo "=== vzr benchmark sweep ===" | tee -a "$ROOT/benchmarks/sweep.log"
echo "  URLs file       : $URLS  (limit=$LIMIT)" | tee -a "$ROOT/benchmarks/sweep.log"
echo "  Keyword         : $KEYWORD" | tee -a "$ROOT/benchmarks/sweep.log"
echo "  Work multiplier : $WORK_MULTIPLIER" | tee -a "$ROOT/benchmarks/sweep.log"
echo "  Repeats         : $REPEATS" | tee -a "$ROOT/benchmarks/sweep.log"
echo "  Worker counts   : $WORKERS_LIST" | tee -a "$ROOT/benchmarks/sweep.log"
echo "  Start           : $(date +%Y-%m-%dT%H:%M:%S%z)" | tee -a "$ROOT/benchmarks/sweep.log"
echo "" | tee -a "$ROOT/benchmarks/sweep.log"

# Common args
COMMON=(--urls "$URLS" --limit "$LIMIT" --keyword "$KEYWORD"
        --work-multiplier "$WORK_MULTIPLIER" --quiet)

run_and_log() {
    local label="$1"; shift
    local i="$1"; shift
    echo "[$(date +%H:%M:%S)] $label  run $i ..." | tee -a "$ROOT/benchmarks/sweep.log"
    local line
    line=$("$@" 2>/dev/null | tail -n 1)   # runner prints one JSON line on stdout
    if [[ -z "$line" ]]; then
        echo "  -> empty output, skipping" | tee -a "$ROOT/benchmarks/sweep.log"
        return
    fi
    # Inject run_idx into the JSON (Python avoids fragile shell string-mangling).
    "$PY" -c "
import json, sys
obj = json.loads(sys.argv[1])
obj['run_idx'] = int(sys.argv[2])
print(json.dumps(obj, ensure_ascii=False))
" "$line" "$i" >> "$OUT"
}

# ---------- serial baseline ----------
for i in $(seq 1 "$REPEATS"); do
    run_and_log "serial" "$i" \
        "$PY" -m src.scraper_serial "${COMMON[@]}"
done

# ---------- MPI sweep ----------
for W in $WORKERS_LIST; do
    # Launch P = W + 1 ranks: W workers plus the rank-0 manager.
    P=$((W + 1))
    for i in $(seq 1 "$REPEATS"); do
        run_and_log "mpi/workers=$W" "$i" \
            "$MPIEXEC" -n "$P" "$PY" -m src.scraper_mpi "${COMMON[@]}"
    done
done

# ---------- Spark sweep ----------
for W in $WORKERS_LIST; do
    for i in $(seq 1 "$REPEATS"); do
        run_and_log "spark/workers=$W" "$i" \
            "$PY" -m src.scraper_spark --workers "$W" "${COMMON[@]}"
    done
done

echo "" | tee -a "$ROOT/benchmarks/sweep.log"
echo "  End             : $(date +%Y-%m-%dT%H:%M:%S%z)" | tee -a "$ROOT/benchmarks/sweep.log"
echo "  Raw results     : $OUT" | tee -a "$ROOT/benchmarks/sweep.log"
echo "Next step: python3 benchmarks/analyze.py"
