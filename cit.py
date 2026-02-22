#!/usr/bin/env python3
"""
Scrape citation counts per paper from a Google Scholar author profile.

Usage:
  pip install requests beautifulsoup4
  python scholar_citations.py \
    --url "https://scholar.google.com/citations?user=kjPIF1cAAAAJ&hl=de" \
    --out citations.csv
"""

import argparse
import csv
import random
import re
import sys
import time
from typing import Dict, List
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

def build_paged_url(base_url: str, cstart: int, pagesize: int) -> str:
    """Append/override cstart & pagesize query params while preserving others."""
    parsed = urlparse(base_url)
    q = parse_qs(parsed.query, keep_blank_values=True)
    q["cstart"] = [str(cstart)]
    q["pagesize"] = [str(pagesize)]
    # Scholar needs view_op=list_works to return the publications table reliably.
    q.setdefault("view_op", ["list_works"])
    new_query = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse(parsed._replace(query=new_query))

def polite_get(session: requests.Session, url: str, max_tries: int = 4, timeout: int = 25) -> requests.Response:
    """GET with basic retries/backoff for 429/5xx."""
    last_exc = None
    for attempt in range(max_tries):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp
            # Backoff for rate limits / transient errors
            if resp.status_code in (429, 500, 502, 503, 504):
                sleep_s = (2 ** attempt) + random.uniform(0, 1.5)
                time.sleep(sleep_s)
            else:
                resp.raise_for_status()
        except requests.RequestException as e:
            last_exc = e
            time.sleep(1.0 + attempt * 0.5)
    if last_exc:
        raise last_exc
    raise RuntimeError("Request failed without an exception.")

def parse_publications(html: str) -> List[Dict]:
    """Parse the publications table rows into dicts."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr.gsc_a_tr")
    pubs = []

    for tr in rows:
        # Title
        title_el = tr.select_one("a.gsc_a_at")
        title = title_el.get_text(strip=True) if title_el else ""

        # Year
        year_el = tr.select_one("td.gsc_a_y span")
        year = year_el.get_text(strip=True) if year_el else ""

        # Citations (anchor when >0, span when 0)
        cit_el = tr.select_one("a.gsc_a_ac, span.gsc_a_ac")
        cit_text = (cit_el.get_text(strip=True) if cit_el else "") or "0"
        # Extract digits, default 0 if dash/empty
        m = re.search(r"\d+", cit_text.replace("\xa0", ""))
        citations = int(m.group(0)) if m else 0

        pubs.append({
            "title": title,
            "year": year,
            "citations": citations,
        })
    return pubs

def scrape_scholar_profile(url: str, pagesize: int = 100, max_pages: int = 100) -> List[Dict]:
    """Iterate paginated profile pages to collect all publications."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
    })

    all_pubs: List[Dict] = []
    seen_titles = set()

    cstart = 0
    for page in range(max_pages):
        page_url = build_paged_url(url, cstart=cstart, pagesize=pagesize)
        resp = polite_get(session, page_url)
        pubs = parse_publications(resp.text)

        # Stop if no more rows
        if not pubs:
            break

        # Deduplicate by title (simple heuristic)
        new_count = 0
        for p in pubs:
            key = p["title"].lower()
            if key and key not in seen_titles:
                seen_titles.add(key)
                all_pubs.append(p)
                new_count += 1

        # Heuristic end condition: if fewer than pagesize new rows appeared, likely done
        if new_count < pagesize:
            break

        # Next page
        cstart += pagesize

        # Be polite
        time.sleep(random.uniform(1.2, 2.4))

    return all_pubs

def save(rows: List[Dict]):
    rows_sorted = sorted(rows, key=lambda r: (-r["citations"], r.get("year") or ""))
    cit = 0
    for r in rows_sorted:
        if r["title"] == "Fast and sensitive protein alignment using DIAMOND" \
            or r["title"] == "Sensitive protein alignments at tree-of-life scale using DIAMOND" \
            or r["title"] == "Sensitive clustering of protein sequences at tree-of-life scale using DIAMOND DeepClust":
                cit += r["citations"]
    f = open("citations.json", "wt")
    f.write('{ "citations": "' + format(cit, ',') + '" }')

def main():
    ap = argparse.ArgumentParser(description="Extract per-paper citation counts from a Google Scholar profile.")
    url = "https://scholar.google.com/citations?user=kjPIF1cAAAAJ&hl=de";
    ap.add_argument("--pagesize", type=int, default=100, help="Items per page (Scholar typically supports up to 100)")
    ap.add_argument("--max-pages", type=int, default=100, help="Safety cap on pages to fetch")
    args = ap.parse_args()

    try:
        pubs = scrape_scholar_profile(url, pagesize=args.pagesize, max_pages=args.max_pages)
        if not pubs:
            print("No publications found. The profile may be empty, blocked, or the HTML structure changed.", file=sys.stderr)
        save(pubs)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()