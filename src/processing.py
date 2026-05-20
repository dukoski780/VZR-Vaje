"""Shared fetch + CPU pipeline used by every runner (serial / MPI / Spark).

Routing all three frameworks through the same per-page code is what makes the
speedup comparison fair: only the parallelisation strategy changes between
runs, never the workload itself.

Per page: HTTP GET, HTML -> text (BeautifulSoup), Unicode-aware tokenisation
(keeps č š ž), a word-frequency counter, text statistics (counts,
type-token ratio, average word length, Flesch reading ease), keyword count,
a language tag from the URL (?lang=en -> "en", else "sl") and the raw body size.

`work_multiplier` repeats the CPU-only steps (HTML->text parse + tokenisation
+ stats) N times. Live HTTP scraping is often network-bound, so amplifying the
CPU work is a deliberate, documented way to push the workload into the
CPU-bound regime where parallel speedup is visible. The parse is inside the
loop so K scales the dominant cost. See the README for benchmarking conventions.
"""

from __future__ import annotations

import re
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any

import requests
from bs4 import BeautifulSoup

WORD_RE = re.compile(r"[A-Za-zČŠŽĆĐčšžćđ]+", re.UNICODE)
SENTENCE_RE = re.compile(r"[.!?]+")
VOWEL_RE = re.compile(r"[aeiouAEIOU]+")

DEFAULT_KEYWORD = "FIS"
DEFAULT_TIMEOUT_S = 15.0
DEFAULT_WORK_MULTIPLIER = 1
USER_AGENT = (
    "vzr-seminar-crawler/1.0 (educational; "
    "Parallel and High Performance Computing course)"
)


@dataclass
class PageResult:
    """Result for one URL.  Always returned, even on failure."""
    url: str
    ok: bool
    status_code: int = 0
    elapsed_fetch_s: float = 0.0
    elapsed_cpu_s: float = 0.0
    # content metrics
    n_words: int = 0
    n_unique_words: int = 0
    type_token_ratio: float = 0.0
    n_sentences: int = 0
    avg_word_len: float = 0.0
    flesch: float = 0.0
    keyword_hits: int = 0
    keyword_present: bool = False
    # page metadata
    language: str = ""          # "sl" or "en"
    page_size_bytes: int = 0    # raw HTTP response body size
    error: str = ""


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def _flesch_reading_ease(n_words: int, n_sentences: int, n_syllables: int) -> float:
    if n_words == 0 or n_sentences == 0:
        return 0.0
    asl = n_words / n_sentences
    asw = n_syllables / n_words
    return 206.835 - 1.015 * asl - 84.6 * asw


def _process_text(text: str, keyword: str) -> dict[str, Any]:
    """The CPU-bound step.  No I/O here."""
    tokens = WORD_RE.findall(text)
    n_words = len(tokens)
    n_sentences = max(1, len(SENTENCE_RE.findall(text)))
    n_syllables = sum(len(VOWEL_RE.findall(w)) for w in tokens)

    avg_word_len = (sum(len(w) for w in tokens) / n_words) if n_words else 0.0
    flesch = _flesch_reading_ease(n_words, n_sentences, n_syllables)

    lower_tokens = [t.lower() for t in tokens]
    counter = Counter(lower_tokens)
    n_unique_words = len(counter)
    type_token_ratio = n_unique_words / n_words if n_words else 0.0

    keyword_hits = counter.get(keyword.lower(), 0)

    return {
        "n_words": n_words,
        "n_unique_words": n_unique_words,
        "type_token_ratio": type_token_ratio,
        "n_sentences": n_sentences,
        "avg_word_len": avg_word_len,
        "flesch": flesch,
        "keyword_hits": keyword_hits,
        "keyword_present": keyword_hits > 0,
    }


def _detect_language(url: str) -> str:
    return "en" if "lang=en" in url else "sl"


def fetch_and_process(
    url: str,
    keyword: str = DEFAULT_KEYWORD,
    timeout: float = DEFAULT_TIMEOUT_S,
    work_multiplier: int = DEFAULT_WORK_MULTIPLIER,
) -> PageResult:
    """Fetch one URL and run the CPU pipeline.  Never raises."""
    language = _detect_language(url)
    t0 = time.perf_counter()
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
        )
        fetch_elapsed = time.perf_counter() - t0
    except Exception as e:
        return PageResult(
            url=url,
            ok=False,
            language=language,
            elapsed_fetch_s=time.perf_counter() - t0,
            error=f"fetch: {type(e).__name__}: {e}",
        )

    if resp.status_code >= 400:
        return PageResult(
            url=url,
            ok=False,
            status_code=resp.status_code,
            language=language,
            page_size_bytes=len(resp.content),
            elapsed_fetch_s=fetch_elapsed,
            error=f"HTTP {resp.status_code}",
        )

    page_size_bytes = len(resp.content)

    cpu_t0 = time.perf_counter()
    try:
        stats: dict[str, Any] = {}
        for _ in range(max(1, work_multiplier)):
            text = _html_to_text(resp.text)
            stats = _process_text(text, keyword)
    except Exception as e:
        return PageResult(
            url=url,
            ok=False,
            status_code=resp.status_code,
            language=language,
            page_size_bytes=page_size_bytes,
            elapsed_fetch_s=fetch_elapsed,
            elapsed_cpu_s=time.perf_counter() - cpu_t0,
            error=f"process: {type(e).__name__}: {e}",
        )
    cpu_elapsed = time.perf_counter() - cpu_t0

    return PageResult(
        url=url,
        ok=True,
        status_code=resp.status_code,
        language=language,
        page_size_bytes=page_size_bytes,
        elapsed_fetch_s=fetch_elapsed,
        elapsed_cpu_s=cpu_elapsed,
        **stats,
    )


def load_urls(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        lines = (ln.strip() for ln in f)
        return [s for s in lines if s and not s.startswith("#")]


def summarize(results: list[PageResult]) -> dict[str, Any]:
    ok = [r for r in results if r.ok]
    n_ok = len(ok)

    sl = [r for r in ok if r.language == "sl"]
    en = [r for r in ok if r.language == "en"]

    def _avg(items, attr):
        return sum(getattr(r, attr) for r in items) / len(items) if items else 0.0

    return {
        "n_total": len(results),
        "n_ok": n_ok,
        "n_failed": len(results) - n_ok,
        # word counts
        "total_words": sum(r.n_words for r in ok),
        "avg_words_per_page": _avg(ok, "n_words"),
        "avg_unique_words_per_page": _avg(ok, "n_unique_words"),
        "avg_type_token_ratio": _avg(ok, "type_token_ratio"),
        # by language
        "n_pages_sl": len(sl),
        "n_pages_en": len(en),
        "avg_words_sl": _avg(sl, "n_words"),
        "avg_words_en": _avg(en, "n_words"),
        "avg_ttr_sl": _avg(sl, "type_token_ratio"),
        "avg_ttr_en": _avg(en, "type_token_ratio"),
        # page size
        "avg_page_size_bytes": _avg(ok, "page_size_bytes"),
        # keyword
        "total_keyword_hits": sum(r.keyword_hits for r in ok),
        "n_pages_with_keyword": sum(1 for r in ok if r.keyword_present),
        # timing
        "avg_fetch_s": _avg(ok, "elapsed_fetch_s"),
        "avg_cpu_s": _avg(ok, "elapsed_cpu_s"),
    }
