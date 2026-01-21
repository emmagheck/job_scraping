import csv
import re
import sys
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

US_STATE_TO_ABBR = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA","Colorado":"CO",
    "Connecticut":"CT","Delaware":"DE","District of Columbia":"DC","Florida":"FL","Georgia":"GA",
    "Hawaii":"HI","Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA","Kansas":"KS","Kentucky":"KY",
    "Louisiana":"LA","Maine":"ME","Maryland":"MD","Massachusetts":"MA","Michigan":"MI","Minnesota":"MN",
    "Mississippi":"MS","Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV","New Hampshire":"NH",
    "New Jersey":"NJ","New Mexico":"NM","New York":"NY","North Carolina":"NC","North Dakota":"ND","Ohio":"OH",
    "Oklahoma":"OK","Oregon":"OR","Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD",
    "Tennessee":"TN","Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA","Washington":"WA","West Virginia":"WV",
    "Wisconsin":"WI","Wyoming":"WY",
}

STATE_ABBR = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS",
    "KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY",
    "NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV",
    "WI","WY","DC"
}


def extract_state(text: str) -> str:
    # Look for ", CA" or "(CA)" patterns
    import re
    m = re.search(r"\b([A-Z]{2})\b", text)
    if m and m.group(1) in STATE_ABBR:
        return m.group(1)
    return ""


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


import time
import requests

def fetch(url: str) -> str:
    last_err = None
    for attempt in range(1, 4):  # 3 attempts
        try:
            r = requests.get(url, headers=DEFAULT_HEADERS, timeout=60)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            # backoff: 2s, 4s, 8s
            time.sleep(2 ** attempt)

    # after retries, raise the last error
    raise last_err



def parse_arl_list_page(html: str, base_url: str):
    soup = BeautifulSoup(html, "html.parser")

    postings = []
    for a in soup.find_all("a"):
        if "Read more" not in clean_text(a.get_text()):
            continue
        href = a.get("href")
        if not href:
            continue
        detail_url = urljoin(base_url, href)

        # Walk upward to the nearest container that includes title + institution + Job Location
        container = a
        for _ in range(6):
            if container.parent:
                container = container.parent

        state = extract_state(text)

        # Title is usually the nearest preceding H3 on the page; try to find it by searching in the container
        title = ""
        h3 = container.find("h3")
        if h3:
            title = clean_text(h3.get_text())
        if not title:
            # fallback: first part of text before "Job Location:"
            title = clean_text(text.split("Job Location:")[0])[:255]

        # Institution usually appears right after title in the container text; use a heuristic:
        # grab the first line after the title that isn't "Job Location..."
        org = ""
        # Try: find the first occurrence of title in text and take what comes after it
        if title and title in text:
            after = text.split(title, 1)[1].strip()
            # org often ends right before "Job Location:"
            if "Job Location:" in after:
                org = clean_text(after.split("Job Location:", 1)[0])
        org = org.replace("Apply By:", "").replace("Date Created:", "").strip()
        if not org:
            org = "Unknown"

        # Job Location: <State>
        state = ""
        m = re.search(r"Job Location:\s*([^A-Za-z]*)([A-Za-z ]+)", text)
        if m:
            state = normalize_state(m.group(2))

        postings.append((title, org, state, detail_url))

    # Find Next » pagination link
    next_url = None
    for a in soup.find_all("a"):
        if clean_text(a.get_text()) == "Next »":
            href = a.get("href")
            if href:
                next_url = urljoin(base_url, href)
            break

    # De-dupe by detail URL
    seen = set()
    out = []
    for t, o, s, u in postings:
        if u in seen:
            continue
        seen.add(u)
        out.append((t, o, s, u))

    return out, next_url


def parse_arl_detail_page(html: str, url: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # ARL pages usually have the main post content in an article/entry area
    main = soup.find("article") or soup.find("main") or soup

    # Prefer a "Description" section if present
    text = clean_text(main.get_text(" "))
    # Keep it shorter so your Django field doesn't get spammed
    return f"{text[:4000]}\n\nSource: {url}"



def scrape_arl(max_pages: int = 5) -> List[JobRow]:
    rows: List[JobRow] = []
    url = ARL_START_URL
    pages = 0

    while url and pages < max_pages:
        pages += 1
        try:
            html = fetch(url)
        except Exception as e:
            print(f"[ERROR] ARL list page fetch failed: {e}", file=sys.stderr)
            break

postings, next_url = parse_arl_list_page(html, url)


        for title, org, state, durl in postings:
            desc = ""
            try:
                detail_html = fetch(durl)
                desc = parse_arl_detail_page(detail_html, durl)
            except Exception as e:
                print(f"[WARN] Failed detail {durl}: {e}", file=sys.stderr)
            remote_type = infer_remote_type(desc)
            rows.append(JobRow(
                title=title[:255],
                organization=org[:255] if org else "Unknown",
                state=state,
                sector="Academic",
                description=desc or f"Source: {durl}"
            ))

        url = next_url

    # De-dupe by title+org
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


def infer_remote_type(text: str) -> str:
    t = text.lower()
    if "fully remote" in t or "100% remote" in t:
        return "Remote"
    if "remote" in t and "hybrid" not in t:
        return "Remote"
    if "hybrid" in t:
        return "Hybrid"
    if "on-site" in t or "onsite" in t or "in person" in t:
        return "Onsite"
    return ""


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
