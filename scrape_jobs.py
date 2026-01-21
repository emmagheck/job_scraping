import csv
import re
import sys
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


ARL_START_URL = "https://www.arl.org/jobs/job-listings/"
DEFAULT_HEADERS = {
    "User-Agent": "EmmaJobBoardBot/1.0 (+https://github.com/yourusername/yourrepo)"
}


@dataclass
class JobRow:
    title: str
    organization: str
    state: str = ""
    sector: str = ""         # You can map these later
    remote_type: str = ""    # Remote/Hybrid/Onsite
    salary_min: str = ""
    salary_max: str = ""
    description: str = ""


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def fetch(url: str) -> str:
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def parse_arl_list_page(html: str, base_url: str) -> (List[str], Optional[str]):
    """
    Returns (detail_urls, next_url)
    """
    soup = BeautifulSoup(html, "html.parser")

    # On the ARL listings page, each posting has a "Read more »" link.
    # We'll gather those detail URLs.
    detail_urls = []
    for a in soup.select('a'):
        txt = clean_text(a.get_text())
        if "Read more" in txt:
            href = a.get("href")
            if href:
                detail_urls.append(urljoin(base_url, href))

    # Find Next » pagination link
    next_url = None
    for a in soup.select("a"):
        if clean_text(a.get_text()) == "Next »":
            href = a.get("href")
            if href:
                next_url = urljoin(base_url, href)
            break

    # De-dupe while preserving order
    seen = set()
    deduped = []
    for u in detail_urls:
        if u not in seen:
            seen.add(u)
            deduped.append(u)

    return deduped, next_url


def parse_arl_detail_page(html: str, url: str) -> JobRow:
    soup = BeautifulSoup(html, "html.parser")

    # Title tends to be the main H1
    title = clean_text((soup.find("h1") or soup.find("h2") or "").get_text() if (soup.find("h1") or soup.find("h2")) else "")
    if not title:
        title = "Untitled"

    # The listing page shows institution + job location; detail pages vary.
    # We'll grab the page text and try to infer organization and state from common patterns.
    full_text = clean_text(soup.get_text(" "))

    # Organization: often appears near the top; attempt common fallback
    organization = ""
    # Try meta or strong labels (best-effort)
    for strong in soup.select("strong"):
        if "Institution" in clean_text(strong.get_text()):
            # next sibling text
            organization = clean_text(strong.parent.get_text().replace(strong.get_text(), ""))
            break
    if not organization:
        # fallback: use first meaningful line-like chunk from the text
        organization = "Unknown"

    # State: look for "Job Location: X" on the page
    state = ""
    m = re.search(r"Job Location:\s*([A-Za-z ]+)", full_text)
    if m:
        state = m.group(1).strip()

    # Put the URL in description so you can click through later
    description = f"{full_text[:5000]}\n\nSource: {url}"

    # ARL is almost always academic/research libraries, so set sector=Academic by default.
    # You can later refine this mapping.
    return JobRow(
        title=title,
        organization=organization[:255],
        state=state[:255],
        sector="Academic",
        description=description
    )


def scrape_arl(max_pages: int = 5) -> List[JobRow]:
    rows: List[JobRow] = []
    url = ARL_START_URL
    pages = 0

    while url and pages < max_pages:
        pages += 1
        html = fetch(url)
        detail_urls, next_url = parse_arl_list_page(html, url)

        for durl in detail_urls:
            try:
                detail_html = fetch(durl)
                row = parse_arl_detail_page(detail_html, durl)
                rows.append(row)
            except Exception as e:
                print(f"[WARN] Failed detail {durl}: {e}", file=sys.stderr)

        url = next_url

    # De-dupe by title + organization
    uniq = {}
    for r in rows:
        key = (r.title.strip().lower(), r.organization.strip().lower())
        uniq[key] = r
    return list(uniq.values())


def scrape_ala_joblist_placeholder() -> List[JobRow]:
    """
    Placeholder because ALA JobLIST is currently serving a maintenance page.
    Once it's back, we can implement either:
      - RSS/custom RSS (if available for searches), or
      - HTML parsing for their results pages.
    """
    return []


def write_csv(rows: List[JobRow], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["title","organization","state","sector","remote_type","salary_min","salary_max","description"]
        )
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "title": r.title,
                "organization": r.organization,
                "state": r.state,
                "sector": r.sector,
                "remote_type": r.remote_type,
                "salary_min": r.salary_min,
                "salary_max": r.salary_max,
                "description": r.description,
            })


if __name__ == "__main__":
    rows = []
    rows += scrape_arl(max_pages=5)
    rows += scrape_ala_joblist_placeholder()

    write_csv(rows, "jobs.csv")
    print(f"Wrote {len(rows)} jobs to jobs.csv")
