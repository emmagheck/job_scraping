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
    source: str = ""          # e.g. "ARL", "ALA JobLIST", "Archives Gig"


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def extract_state_from_detail(text: str) -> str:
    """
    Pull a US state from ARL detail page text.
    Tries structured labels first ("Job Location: New Jersey"),
    then falls back to scanning for ", ST" or "(ST)" patterns.
    """
    # Structured label — ARL detail pages repeat "Job Location: StateName"
    m = re.search(r"Job Location[:\s]+([A-Za-z][A-Za-z ]+?)(?:\s{2,}|,|\n|$)", text, re.IGNORECASE)
    if m:
        candidate = clean_text(m.group(1))
        if candidate.upper() in STATE_ABBR:
            return candidate.upper()
        for full, abbr in US_STATE_TO_ABBR.items():
            if full.lower() == candidate.lower():
                return abbr
        if any(x in candidate.lower() for x in {"washington dc", "district of columbia"}):
            return "DC"

    # Fallback: "City, ST" pattern — find the LAST match to avoid false positives in body text
    matches = list(re.finditer(r",\s*([A-Z]{2})\b", text))
    for m in reversed(matches):
        abbr = m.group(1)
        if abbr in STATE_ABBR:
            return abbr

    return ""


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

        # State name after "Job Location:"
        # ARL uses full state names ("New York") but also "City, ST" combos.
        # The list-page text blobs everything together so we stop at known terminators.
        state = ""
        m = re.search(
            r"Job Location:\s*([A-Za-z][A-Za-z ,\.]+?)(?:\s+(?:Apply By|Date Created|Read more)|$)",
            text,
            re.IGNORECASE,
        )
        if m:
            loc_raw = clean_text(m.group(1))
            # Try each comma-separated part — reversed so "New York, NY" hits "NY" first
            for part in reversed(loc_raw.split(",")):
                part = part.strip()
                if part.upper() in STATE_ABBR:
                    state = part.upper()
                    break
                for full, abbr in US_STATE_TO_ABBR.items():
                    if full.lower() == part.lower():
                        state = abbr
                        break
                if state:
                    break
            if not state and any(x in loc_raw.lower() for x in {"washington dc", "district of columbia"}):
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
        except Exception as e:
            print(f"[ERROR] ARL list page fetch failed: {e}", file=sys.stderr)
            break

        for title, org, state, durl in postings:
            desc = ""
            date_posted = ""
            try:
                detail_html = fetch(durl)
                desc = parse_arl_detail_page(detail_html, durl)
                date_posted = extract_date_posted(desc)

                # Fallback: if list page didn't give us a state, try the detail page text
                if not state:
                    state = extract_state_from_detail(desc)

            except Exception as e:
                print(f"[WARN] Failed detail {durl}: {e}", file=sys.stderr)

            remote_type = infer_remote_type(desc)

            # Infer sector from title + org + description rather than hardcoding "Academic"
            sector = infer_sector_from_text(f"{title} {org} {desc}")

            rows.append(JobRow(
                title=title[:255],
                organization=org[:255] if org else "Unknown",
                state=state,
                sector=sector,
                remote_type=remote_type,
                date_posted=date_posted,
                apply_url=durl,
                description=desc or f"Source: {durl}",
                source="ARL",
            ))

        url = next_url

    # De-dupe by title+org
    uniq = {}
    for r in rows:
        key = (r.title.strip().lower(), r.apply_url.strip().lower())
        uniq[key] = r
    return list(uniq.values())


ALA_JOBLIST_BASE = "https://joblist.ala.org"
ALA_JOBLIST_SEARCH = "https://joblist.ala.org/jobs/"

# ALA JobLIST sector keywords → our canonical sector values
ALA_SECTOR_MAP = {
    "academic": "Academic",
    "university": "Academic",
    "college": "Academic",
    "school": "Academic",
    "public library": "Public",
    "public": "Public",
    "government": "Government",
    "federal": "Government",
    "state library": "Government",
    "special": "Other",
    "corporate": "Corporate",
    "nonprofit": "Nonprofit",
    "museum": "Museum",
    "medical": "Medical",
    "hospital": "Medical",
    "health": "Medical",
    "law": "Other",
    "archive": "Other",
}


def infer_sector_from_text(text: str) -> str:
    """Guess sector from job title / org / description text."""
    t = text.lower()
    # Order matters — check more specific terms first
    for keyword, sector in ALA_SECTOR_MAP.items():
        if keyword in t:
            return sector
    return ""


def parse_ala_salary(text: str):
    """
    Extract salary_min and salary_max from strings like:
      "$55,000 - $70,000"  "$80,000+"  "Commensurate with experience"
    Returns (salary_min_str, salary_max_str) — both may be empty strings.
    """
    # Strip commas and find dollar amounts
    nums = re.findall(r"\$[\d,]+", text)
    cleaned = [int(n.replace("$", "").replace(",", "")) for n in nums]
    if len(cleaned) >= 2:
        return str(cleaned[0]), str(cleaned[1])
    if len(cleaned) == 1:
        return str(cleaned[0]), ""
    return "", ""


def parse_ala_list_page(html: str) -> list:
    """
    Parse one page of ALA JobLIST search results.
    Returns list of (title, org, state, detail_url) tuples.

    ALA JobLIST (Jobiqo platform) renders each result as an <article>
    or a list item with class "views-row". We try both patterns so this
    stays resilient if they tweak their markup.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Pattern A: <article> tags (Jobiqo standard)
    articles = soup.find_all("article")

    # Pattern B: fallback — divs/lis with a job-title link
    if not articles:
        articles = soup.find_all(class_=re.compile(r"views-row|job[-_]result|job[-_]listing"))

    for article in articles:
        # Title + detail URL
        title_tag = (
            article.find("h2")
            or article.find("h3")
            or article.find(class_=re.compile(r"job[-_]title|title|views-field-title"))
        )
        if not title_tag:
            continue
        link = title_tag.find("a", href=True) or article.find("a", href=True)
        if not link:
            continue

        title = clean_text(title_tag.get_text())
        detail_url = urljoin(ALA_JOBLIST_BASE, link["href"])

        # Organisation
        org_tag = article.find(class_=re.compile(r"organization|employer|company|field-name-field-job-organization"))
        org = clean_text(org_tag.get_text()) if org_tag else "Unknown"

        # Location → state
        loc_tag = article.find(class_=re.compile(r"location|field-name-field-job-location|city"))
        loc_text = clean_text(loc_tag.get_text()) if loc_tag else ""
        state = ""
        if loc_text:
            # Try full state name first, then abbreviation
            for full, abbr in US_STATE_TO_ABBR.items():
                if full.lower() in loc_text.lower():
                    state = abbr
                    break
            if not state:
                m = re.search(r"\b([A-Z]{2})\b", loc_text)
                if m and m.group(1) in STATE_ABBR:
                    state = m.group(1)

        results.append((title, org, state, detail_url))

    # De-dupe within the page by detail URL
    seen = set()
    out = []
    for item in results:
        if item[3] not in seen:
            seen.add(item[3])
            out.append(item)
    return out


def parse_ala_next_url(html: str, current_url: str) -> str:
    """Return the URL of the next results page, or empty string."""
    soup = BeautifulSoup(html, "html.parser")
    # Jobiqo uses rel="next" on pagination links
    next_link = soup.find("a", rel="next") or soup.find("a", string=re.compile(r"next|›|»", re.I))
    if next_link and next_link.get("href"):
        return urljoin(current_url, next_link["href"])
    return ""


def parse_ala_detail_page(html: str, url: str):
    """
    Scrape a single ALA JobLIST job detail page.
    Returns (description, date_posted, salary_min, salary_max, sector, remote_type).
    """
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup.find("article") or soup

    full_text = clean_description(clean_text(main.get_text(" ")))

    # Date posted — ALA uses labels like "Date Posted:", "Posted:", "Closing Date:"
    date_posted = ""
    m = re.search(r"(?:date\s*posted|posted\s*on|posted)[\s:]+([A-Za-z]+\s+\d{1,2},?\s+\d{4})", full_text, re.I)
    if m:
        for fmt in ("%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y"):
            try:
                date_posted = datetime.strptime(m.group(1).strip(), fmt).date().isoformat()
                break
            except ValueError:
                continue
    if not date_posted:
        # Try numeric date like 03/15/2026
        m = re.search(r"(?:date\s*posted|posted)[\s:]+(\d{1,2}/\d{1,2}/\d{4})", full_text, re.I)
        if m:
            try:
                date_posted = datetime.strptime(m.group(1), "%m/%d/%Y").date().isoformat()
            except ValueError:
                pass

    # Salary
    salary_min, salary_max = "", ""
    m = re.search(r"(?:salary|compensation|pay)[\s:]+([^\n]{5,80})", full_text, re.I)
    if m:
        salary_min, salary_max = parse_ala_salary(m.group(1))

    # Sector — infer from full text
    sector = infer_sector_from_text(full_text)

    # Remote type
    remote_type = infer_remote_type(full_text)

    description = f"{full_text[:4000]}\n\nSource: {url}"
    return description, date_posted, salary_min, salary_max, sector, remote_type


def scrape_ala_joblist(max_pages: int = 10) -> List[JobRow]:
    """
    Scrape ALA JobLIST (joblist.ala.org).
    Paginates through search results, then fetches each detail page.
    """
    rows: List[JobRow] = []
    url = ALA_JOBLIST_SEARCH
    pages = 0

    while url and pages < max_pages:
        pages += 1
        try:
            html = fetch(url)
            postings = parse_ala_list_page(html)
            next_url = parse_ala_next_url(html, url)
            print(f"[INFO] ALA JobLIST page {pages}: found {len(postings)} postings", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] ALA JobLIST list page failed: {e}", file=sys.stderr)
            break

        if not postings:
            print(f"[WARN] ALA JobLIST page {pages}: no postings parsed — site structure may have changed", file=sys.stderr)
            break

        for title, org, state, durl in postings:
            desc, date_posted, salary_min, salary_max, sector, remote_type = "", "", "", "", "", ""
            try:
                detail_html = fetch(durl)
                desc, date_posted, salary_min, salary_max, sector, remote_type = parse_ala_detail_page(detail_html, durl)
            except Exception as e:
                print(f"[WARN] ALA JobLIST detail fetch failed {durl}: {e}", file=sys.stderr)
                desc = f"Source: {durl}"

            # If sector inference failed, try from title + org
            if not sector:
                sector = infer_sector_from_text(f"{title} {org}")

            rows.append(JobRow(
                title=title[:255],
                organization=org[:255],
                state=state,
                sector=sector,
                remote_type=remote_type,
                salary_min=salary_min,
                salary_max=salary_max,
                date_posted=date_posted,
                apply_url=durl,
                description=desc,
                source="ALA JobLIST",
            ))

        url = next_url

    # De-dupe within source by title + url
    uniq = {}
    for r in rows:
        key = (r.title.strip().lower(), r.apply_url.strip().lower())
        uniq[key] = r

    print(f"[INFO] ALA JobLIST: {len(uniq)} unique jobs scraped", file=sys.stderr)
    return list(uniq.values())


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

def parse_archivesgig_org(title: str, html_body: str) -> str:
    """
    Extract organisation name from an Archives Gig entry.
    Tries the structured "Name of Employer:" HTML field first,
    then falls back to the title pattern "City, ST: Job Title, Org Name".
    """
    # 1. Structured HTML field
    soup = BeautifulSoup(html_body, "html.parser")
    for strong in soup.find_all("strong"):
        if "name of employer" in strong.get_text().lower():
            parent = strong.parent
            text = parent.get_text(" ")
            m = re.search(r"Name of Employer[:\s]+(.+)", text, re.IGNORECASE)
            if m:
                org = clean_text(m.group(1))
                if org:
                    return org[:255]
            break

    # 2. Title pattern: "City, ST: Job Title, Org Name"  or  "City, ST/Remote: Job Title, Org Name"
    #    Split on the first colon to drop the location prefix, then take everything after the last comma
    if ":" in title:
        after_colon = title.split(":", 1)[1].strip()
        # The org is after the last comma in the remaining string
        if "," in after_colon:
            org = after_colon.rsplit(",", 1)[-1].strip()
            if org:
                return org[:255]

    return "Unknown"


def parse_archivesgig_sector(title: str, org: str, html_body: str) -> str:
    """
    Infer sector from org name, job title, and description text.
    Archives Gig doesn't publish a sector field so we rely on keyword matching.
    """
    combined = f"{title} {org} {html_body}".lower()

    # Museum / cultural institution — check before academic so "university art museum" → Museum
    if any(w in combined for w in ["museum", "gallery", "historic", "historical society",
                                    "preservation", "heritage", "cultural center"]):
        return "Museum"

    # Academic
    if any(w in combined for w in ["university", "college", "academic", "school of",
                                    "institute of technology", "suny", "cuny"]):
        return "Academic"

    # Government
    if any(w in combined for w in ["state archive", "national archive", "federal", "government",
                                    "state library", "congressional", "municipal", "county"]):
        return "Government"

    # Medical
    if any(w in combined for w in ["hospital", "medical", "health system", "clinic"]):
        return "Medical"

    # Nonprofit
    if any(w in combined for w in ["foundation", "society", "alliance", "association",
                                    "institute", "nonprofit", "non-profit", "conservancy",
                                    "trust", "fund "]):
        return "Nonprofit"

    # Public library
    if "public library" in combined or "public libraries" in combined:
        return "Public"

    # Corporate
    if any(w in combined for w in ["corporation", "inc.", "llc", "ltd", "corporate"]):
        return "Corporate"

    return "Other"


def scrape_archivesgig(max_items: int = 80) -> List[JobRow]:
    rows: List[JobRow] = []
    d = feedparser.parse(ARCHIVESGIG_RSS)

    for entry in (d.entries or [])[:max_items]:
        title = clean_text(getattr(entry, "title", ""))[:255]
        url = getattr(entry, "link", "") or ""

        # Prefer full HTML content if available, otherwise summary
        body = getattr(entry, "summary", "") or ""
        if hasattr(entry, "content") and entry.content:
            try:
                body = entry.content[0].value
            except Exception:
                pass

        body_clean = clean_text(body)

        org = parse_archivesgig_org(title, body)
        sector = parse_archivesgig_sector(title, org, body_clean)

        text_for_inference = f"{title} {org} {body_clean}"
        state = extract_state(text_for_inference)
        remote_type = infer_remote_type(text_for_inference)
        date_posted = iso_date_from_entry(entry)

        # Strip HTML tags for the stored description
        soup = BeautifulSoup(body, "html.parser")
        desc_text = clean_text(soup.get_text(" "))

        rows.append(JobRow(
            title=title,
            organization=org,
            state=state,
            sector=sector,
            remote_type=remote_type,
            salary_min="",
            salary_max="",
            date_posted=date_posted,
            apply_url=url,
            description=(desc_text[:4000] + (f"\n\nSource: {url}" if url else "")),
            source="Archives Gig",
        ))

    return rows


def write_csv(rows: List[JobRow], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "organization", "state", "sector", "remote_type", "salary_min", "salary_max", "date_posted", "apply_url", "description", "source"])
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
                "source": r.source,
            })


if __name__ == "__main__":
    rows = []
    rows += scrape_arl(max_pages=5)
    rows += scrape_ala_joblist(max_pages=10)
    rows += scrape_archivesgig(max_items=80)

    write_csv(rows, "jobs.csv")
    print(f"Wrote {len(rows)} jobs to jobs.csv")
