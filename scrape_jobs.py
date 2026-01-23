import csv
import re
import sys
import feedparser
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urljoin
from datetime import datetime, timezone

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

ARCHIVESGIG_RSS = "https://archivesgig.com/feed/"


def extract_state(text: str) -> str:
    # Look for ", CA" or "(CA)" patterns
    import re
    m = re.search(r"\b([A-Z]{2})\b", text)
    if m and m.group(1) in STATE_ABBR:
        return m.group(1)
    return ""


def normalize_state(name: str) -> str:
    name = clean_text(name)
    if not name:
        return ""
    # handle Washington DC labeling on ARL page
    if name.lower() in {"washington dc", "district of columbia"}:
        return "DC"
    return US_STATE_TO_ABBR.get(name, "")


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
    date_posted: str = ""
    description: str = ""
    apply_url: str = ""


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

    # Each posting is a <li> that contains an <h3> and a link whose text includes "Read more"
    for li in soup.find_all("li"):
        h3 = li.find("h3")
        if not h3:
            continue

        title = clean_text(h3.get_text())

        # Find the "Read more" link by checking link text (more reliable than string=)
        readmore = None
        for a in li.find_all("a", href=True):
            if "read more" in clean_text(a.get_text()).lower():
                readmore = a
                break
        if not readmore:
            continue

        text = clean_text(li.get_text(" "))

        # Org = text between title and "Job Location:"
        org = "Unknown"
        if "Job Location:" in text:
            before_loc = text.split("Job Location:", 1)[0]
            if before_loc.startswith(title):
                before_loc = before_loc[len(title):].strip()
            org = clean_text(before_loc) or "Unknown"

        # State name after "Job Location:" (ARL uses full state names like "New York")
        state = ""
        m = re.search(r"Job Location:\s*([A-Za-z ]+)", text)
        if m:
            state_name = clean_text(m.group(1))
            state = US_STATE_TO_ABBR.get(state_name, "")
            if state_name.lower() in {"washington dc", "district of columbia"}:
                state = "DC"

        detail_url = urljoin(base_url, readmore["href"])
        postings.append((title, org, state, detail_url))

    # Pagination
    next_url = None
    for a in soup.find_all("a", href=True):
        if clean_text(a.get_text()) == "Next »":
            next_url = urljoin(base_url, a["href"])
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

def extract_date_posted(text: str) -> str:
    # Match: Date Created: 01/22/2026
    m = re.search(r"Date Created:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", text, flags=re.I)
    if m:
        dt = datetime.strptime(m.group(1), "%m/%d/%Y").date()
        return dt.isoformat()  # YYYY-MM-DD

    # Match: Date Created: January 22, 2026
    m = re.search(r"Date Created:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", text, flags=re.I)
    if m:
        dt = datetime.strptime(m.group(1), "%B %d, %Y").date()
        return dt.isoformat()

    return ""


def clean_description(text: str) -> str:
    junk_phrases = [
        "share",
        "tweet",
        "email",
        "print",
        "facebook",
        "linkedin",
    ]
    for phrase in junk_phrases:
        text = re.sub(rf"\b{phrase}\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()



def parse_arl_detail_page(html: str, url: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # ARL pages usually have the main post content in an article/entry area
    main = soup.find("article") or soup.find("main") or soup

    # Prefer a "Description" section if present
    text = clean_description(clean_text(main.get_text(" ")))
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
            postings, next_url = parse_arl_list_page(html, url)
            print(f"[INFO] ARL page {pages}: found {len(postings)} postings", file=sys.stderr)
            print(f"[INFO] next_url: {next_url}", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] ARL list page fetch failed: {e}", file=sys.stderr)
            break

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
                remote_type=remote_type,
                description=desc or f"Source: {durl}",
                apply_url=durl,  # keep a URL for dedupe/import even if it's just the detail page
            ))

        # IMPORTANT: this must run once per page (not inside the for-loop)
        url = next_url

    return rows

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

def parse_date_any(s: str) -> str:
    """Return YYYY-MM-DD or empty string."""
    if not s:
        return ""
    try:
        # feedparser often provides a parsed struct_time too, but this is a safe fallback
        dt = datetime(*feedparser._parse_date(s)[:6], tzinfo=timezone.utc)  # type: ignore
        return dt.date().isoformat()
    except Exception:
        return ""

ARCHIVESGIG_RSS = "https://archivesgig.com/feed/"

def iso_date_from_entry(entry) -> str:
    """Return YYYY-MM-DD or empty string."""
    # feedparser gives you struct_time in *published_parsed* / *updated_parsed* when available
    tm = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not tm:
        return ""
    try:
        return datetime(tm.tm_year, tm.tm_mon, tm.tm_mday).date().isoformat()
    except Exception:
        return ""

def canonicalize(s: str) -> str:
    s = clean_text(s).lower()
    s = s.replace("—", "-").replace("–", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_archivesgig_title(raw_title: str):
    """
    ArchivesGig titles often look like:
      'City, ST: Job Title, Organization'
      'Remote: Job Title, Organization'
      'Job Title, Organization'
    Return: (title, org, state_guess)
    """
    t = clean_text(raw_title)
    state_guess = ""

    # City, ST: ...
    m = re.match(r"^.*,\s*([A-Z]{2})\s*:\s*(.*)$", t)
    if m and m.group(1) in STATE_ABBR:
        state_guess = m.group(1)
        t = m.group(2)

    # Remote/Hybrid/Onsite prefix
    t = re.sub(r"^(remote|hybrid|onsite|on-site|in person)\s*:\s*", "", t, flags=re.I)

    # Split on last comma -> org is often the last chunk
    org = ""
    title = t
    if ", " in t:
        title, org = t.rsplit(", ", 1)

    return clean_text(title), clean_text(org), state_guess

def scrape_archivesgig(max_items: int = 80) -> List[JobRow]:
    rows: List[JobRow] = []
    d = feedparser.parse(ARCHIVESGIG_RSS)

    for entry in (d.entries or [])[:max_items]:
        raw_title = getattr(entry, "title", "") or ""
        title, org_from_title, state_from_title = parse_archivesgig_title(raw_title)

        url = getattr(entry, "link", "") or ""

        # Prefer full content if available, otherwise summary
        body = clean_text(getattr(entry, "summary", ""))
        if hasattr(entry, "content") and entry.content:
            try:
                body = clean_text(entry.content[0].value)
            except Exception:
                pass

        text_for_inference = f"{title} {org_from_title} {body}"

        # Prefer the state in the prefix if present, otherwise infer
        state = state_from_title or extract_state(text_for_inference)

        remote_type = infer_remote_type(text_for_inference)
        date_posted = iso_date_from_entry(entry)

        rows.append(JobRow(
            title=title[:255] if title else clean_text(raw_title)[:255],
            organization=org_from_title[:255] if org_from_title else "Unknown",
            state=state,
            sector="Other",
            remote_type=remote_type,
            salary_min="",
            salary_max="",
            date_posted=date_posted,
            apply_url=url,
            description=(body[:4000] + (f"\n\nSource: {url}" if url else ""))
        ))

    return rows

HIGHEREDJOBS_FEEDS = [
    ("https://www.higheredjobs.com/rss/categoryFeed.cfm?catID=182", "HigherEdJobs (Library & Info Science)"),
    ("https://www.higheredjobs.com/rss/categoryFeed.cfm?catID=34",  "HigherEdJobs (catID 34)"),
]

def split_title_org(raw_title: str):
    """
    Many job feeds use patterns like:
      'Job Title - University Name'
      'Job Title — University Name'
    Returns (title, org)
    """
    t = clean_text(raw_title)
    for sep in [" — ", " - ", " – "]:
        if sep in t:
            left, right = t.split(sep, 1)
            left, right = clean_text(left), clean_text(right)
            # avoid bad splits on titles that contain hyphens
            if left and right and len(right) <= 80:
                return left, right
    return t, ""

def scrape_higheredjobs_feed(feed_url: str, source_label: str, max_items: int = 100) -> List[JobRow]:
    """
    RSS -> JobRow
    Uses entry.title, entry.link, entry.summary/description/content, entry.published.
    """
    rows: List[JobRow] = []

    # Fetch XML ourselves so we can control headers/timeouts (some feeds dislike feedparser's default)
    try:
        xml = fetch(feed_url)
    except Exception as e:
        print(f"[ERROR] HigherEdJobs fetch failed ({source_label}): {e}", file=sys.stderr)
        return rows

    d = feedparser.parse(xml)
    entries = d.entries or []
    print(f"[INFO] {source_label}: found {len(entries)} entries", file=sys.stderr)

    for entry in entries[:max_items]:
        raw_title = getattr(entry, "title", "") or ""
        link = getattr(entry, "link", "") or ""

        # Prefer full content -> summary
        body = clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        if hasattr(entry, "content") and entry.content:
            try:
                body = clean_text(entry.content[0].value)
            except Exception:
                pass

        # Try to pull organization from title; otherwise leave Unknown
        title, org_from_title = split_title_org(raw_title)

        text_for_inference = f"{title} {org_from_title} {body}"
        state = extract_state(text_for_inference)
        remote_type = infer_remote_type(text_for_inference)
        date_posted = iso_date_from_entry(entry)

        rows.append(JobRow(
            title=title[:255] if title else clean_text(raw_title)[:255],
            organization=(org_from_title[:255] if org_from_title else "Unknown"),
            state=state,
            sector="Academic",
            remote_type=remote_type,
            salary_min="",
            salary_max="",
            date_posted=date_posted,
            apply_url=link,  # HigherEdJobs link is a good “apply/source” link
            description=(body[:4000] + (f"\n\nSource: {link}" if link else "")),
        ))

    return rows

def scrape_higheredjobs_all(max_items_each: int = 100) -> List[JobRow]:
    rows: List[JobRow] = []
    for url, label in HIGHEREDJOBS_FEEDS:
        rows += scrape_higheredjobs_feed(url, label, max_items=max_items_each)
    return rows


def write_csv(rows: List[JobRow], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "organization", "state", "sector", "remote_type", "salary_min", "salary_max", "date_posted", "apply_url", "description"])
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
                "date_posted": r.date_posted,
                "apply_url": r.apply_url,
                "description": r.description,
            })

def dedupe_rows(rows: List[JobRow]) -> List[JobRow]:
    uniq = {}

    for r in rows:
        title_key = canonicalize(r.title)
        org_key = canonicalize(r.organization)

        if not org_key or org_key in {"unknown", "n/a", "-"}:
            key = ("url", canonicalize(r.apply_url))
        else:
            key = ("job", title_key, org_key)

        existing = uniq.get(key)
        if not existing:
            uniq[key] = r
            continue

        def score(x: JobRow) -> int:
            s = 0
            if x.apply_url: s += 3
            if x.date_posted: s += 2
            if x.state: s += 1
            if x.remote_type: s += 1
            if x.salary_min or x.salary_max: s += 1
            s += min(len(x.description or ""), 2000) // 500
            return s

        if score(r) > score(existing):
            uniq[key] = r

    return list(uniq.values())


if __name__ == "__main__":
    rows = []
    rows += scrape_arl(max_pages=5)
    rows += scrape_ala_joblist_placeholder()
    rows += scrape_archivesgig(max_items=80)
    rows += scrape_higheredjobs_all(max_items_each=120)
    rows = dedupe_rows(rows)
    write_csv(rows, "jobs.csv")
    print(f"Wrote {len(rows)} jobs to jobs.csv")
