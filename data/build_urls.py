"""Build data/urls.txt from the FIS UNM sitemap.

The site (WordPress + WPML) exposes a sitemap index of sub-sitemaps; each
already lists both the Slovenian originals and their English (?lang=en)
counterparts. We parse every sub-sitemap, skip attachment/media entries,
deduplicate (preserving order), and write one URL per line. The natural mix
of short and long pages is what the Manager-Worker load balancing targets.

    python data/build_urls.py [--verbose]
"""

from __future__ import annotations

import argparse
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

SITEMAP_INDEX = "https://www.fis.unm.si/sitemap_index.xml"
OUT_PATH = Path(__file__).parent / "urls.txt"
TIMEOUT = 20

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

USER_AGENT = (
    "vzr-seminar-crawler/1.0 (educational; "
    "Parallel and High Performance Computing course)"
)

SKIP_RE = re.compile(r"attachment|media|image|wp-content", re.IGNORECASE)


def _get(url: str) -> bytes:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.content


def _locs(xml_bytes: bytes) -> list[str]:
    root = ET.fromstring(xml_bytes)
    return [
        e.text.strip()
        for e in root.findall(".//sm:loc", NS)
        if e.text and not SKIP_RE.search(e.text)
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    print(f"Fetching sitemap index: {SITEMAP_INDEX}")
    sub_sitemaps = _locs(_get(SITEMAP_INDEX))
    print(f"Found {len(sub_sitemaps)} sub-sitemaps")

    all_urls: list[str] = []
    for sm_url in sub_sitemaps:
        if args.verbose:
            print(f"  {sm_url}")
        urls = _locs(_get(sm_url))
        all_urls.extend(urls)
        if args.verbose:
            print(f"    -> {len(urls)} URLs")

    # Deduplicate, preserve order.
    seen: set[str] = set()
    unique: list[str] = []
    for u in all_urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)

    en = sum(1 for u in unique if "lang=en" in u)
    sl = len(unique) - en

    OUT_PATH.write_text("\n".join(unique) + "\n", encoding="utf-8")
    print(f"\nWrote {len(unique)} URLs to {OUT_PATH}")
    print(f"  {sl} Slovenian  +  {en} English (?lang=en)  [from sitemap, WPML]")


if __name__ == "__main__":
    main()
