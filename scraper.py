"""
Daily scraper: fetches the latest YourStory funding articles and saves to PostgreSQL.
Runs via GitHub Actions cron (6 AM IST daily).
Only fetches page 1 (12 most recent articles).
"""
import os
import re
import sys
import logging
from datetime import datetime, timezone

import cloudscraper
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

API_URL = "https://yourstory.com/api/v2/category/stories"
LIMIT = 12


def extract_funding_amount(title, excerpt=""):
    text = f"{title} {excerpt}"
    for pat in [
        r"\$[\d,.]+\s*(?:billion|Bn|B)\b", r"\$[\d,.]+\s*(?:million|Mn|M)\b",
        r"\$[\d,.]+\s*(?:thousand|K)\b", r"\$[\d,.]+",
        r"Rs\.?\s*[\d,.]+\s*(?:crore|Cr)\b", r"Rs\.?\s*[\d,.]+\s*(?:lakh|L)\b",
        r"₹\s*[\d,.]+\s*(?:crore|Cr)\b", r"₹\s*[\d,.]+\s*(?:lakh|L)\b",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(0).strip()
    return None


DESCRIPTOR_PREFIXES = re.compile(
    r"^(?:"
    r"[\w]+-based|AI-powered|tech-enabled|"
    r"(?:tech|fintech|healthtech|agritech|edtech|SaaS|B2B|B2C|D2C|EV|AI|IoT|"
    r"deeptech|cleantech|medtech|proptech|insurtech|legaltech|biotech|neobank(?:ing)?|"
    r"crypto|blockchain|drone|logistics|e-?commerce|social commerce|"
    r"housing finance|digital lending|digital health|"
    r"cloud kitchen|food delivery|quick commerce|"
    r"electric vehicle|electric mobility|"
    r"venture capital|private equity|"
    r"home decor|personal care|pet care|beauty|fashion|gaming|"
    r"HR|HRtech|recruitment|staffing|"
    r"travel|mobility|automotive|aerospace|defence|"
    r"supply chain|procurement|marketing|advertising|"
    r"cybersecurity|data analytics|"
    r"(?:K-?12|ed-?tech|online (?:education|learning))"
    r")"
    r"(?:\s+\w+)*"  # optional extra words between sector and entity type
    r"\s+(?:startup|firm|company|platform|lender|player|unicorn|soonicorn|"
    r"major|giant|leader|provider|maker|brand|chain|venture|"
    r"aggregator|marketplace|enterprise|solution|solutions)"
    r")\s+",
    re.IGNORECASE,
)

# Tags to skip when looking for company names in tags
SKIP_TAGS = {
    "just in", "funding", "funding news", "startup", "startups",
    "weekly funding roundup", "indian startup funding",
    "artificiai intelligence", "artificial intelligence", "ai",
    "series a", "series b", "series c", "series d", "series e",
    "seed funding", "pre-seed", "debt funding",
    "venture capital", "private equity",
}


def _clean_company_name(name):
    """Strip descriptive prefixes/suffixes from an extracted company name."""
    name = name.strip()
    # Remove leading descriptors like "Tech startup", "Fintech firm"
    name = DESCRIPTOR_PREFIXES.sub("", name)
    # Also handle simpler forms: "startup X", "platform X"
    name = re.sub(
        r"^(?:startup|firm|company|platform|lender)\s+",
        "", name, flags=re.IGNORECASE,
    )
    # Remove trailing descriptors like "'s EV bus platform"
    name = re.sub(r"['\u2019]s\s+.*$", "", name)
    return name.strip()


def _company_from_tags(tags):
    """Try to find a company name from tags (first non-generic tag)."""
    if not tags:
        return None
    for tag in tags:
        if not isinstance(tag, dict) or "name" not in tag:
            continue
        t = tag["name"].strip()
        if t.lower() in SKIP_TAGS:
            continue
        # Skip tags that look like categories/sectors (all lowercase, very generic)
        if re.match(r"^[a-z\s-]+$", t) and len(t.split()) <= 2:
            continue
        # Good candidate: proper-cased, looks like a name
        if 2 <= len(t) <= 80:
            return t
    return None


def extract_company_name(title, excerpt="", tags=None):
    clean = re.sub(r"^\[.*?\]\s*", "", title)
    # Pattern 1: "CompanyName raises/secures/bags ..."
    for pat in [
        r"^(.+?)\s+(?:raises?|secures?|gets?|bags?|closes?|receives?|lands?|nabs?|grabs?|clinches?|acquires?)\b",
        r"^(.+?)\s+(?:funding|investment|round)\b",
    ]:
        m = re.search(pat, clean, re.IGNORECASE)
        if m:
            name = _clean_company_name(m.group(1))
            if 2 <= len(name) <= 100:
                return name
    # Pattern 2: "Investor invests/leads/backs $X in CompanyName"
    m = re.search(
        r"(?:invests?|injects?|infuses?|commits?|pumps?|leads?|backs?)\s+.{3,30}?\s+in(?:to)?\s+(.+?)(?:\s+to\b|\s*$)",
        clean, re.IGNORECASE,
    )
    if m:
        name = _clean_company_name(m.group(1).rstrip("."))
        if name.lower() in ("mid-market companies", "indian startups", "startups"):
            name = None
        if name and 2 <= len(name) <= 80:
            return name
    # Pattern 3: "Investor doubles down on CompanyName"
    m = re.search(r"doubles down on\s+(.+?)(?:,|\s*$)", clean, re.IGNORECASE)
    if m:
        name = _clean_company_name(m.group(1))
        if 2 <= len(name) <= 80:
            return name
    # Pattern 4: Try the same patterns on the excerpt
    if excerpt:
        for pat in [
            r"(\b[A-Z][\w]*(?:\s+[A-Z][\w]*)*)\s+(?:has\s+)?(?:raised?|secured?|closed?|received?)\b",
        ]:
            m = re.search(pat, excerpt)
            if m:
                name = _clean_company_name(m.group(1))
                if 2 <= len(name) <= 80:
                    return name
    # Pattern 5: Fallback to tags — first non-generic tag is often the company
    tag_name = _company_from_tags(tags)
    if tag_name:
        return tag_name
    return None


def normalize(raw):
    md = raw.get("metadata", {})
    title = raw.get("title", "").replace("\r\n", " ").replace("\n", " ").strip()
    excerpt = (md.get("excerpt", "") or "").replace("\r\n", " ").replace("\n", " ").strip()
    path = raw.get("path", "") or f"/{raw.get('slug', '')}"
    authors = md.get("authors", [])
    cat = md.get("category", {})
    tags = [t["name"] for t in md.get("tags", []) if isinstance(t, dict) and "name" in t]
    return {
        "story_id": raw.get("id"),
        "title": title,
        "slug": raw.get("slug", ""),
        "url": f"https://yourstory.com{path}",
        "excerpt": excerpt,
        "published_at": raw.get("publishedAt") or raw.get("published_at"),
        "image_url": md.get("media") or "",
        "author_name": authors[0].get("name") if authors else None,
        "category": cat.get("name") if isinstance(cat, dict) else None,
        "tags": tags or None,
        "time_to_read": md.get("timeToRead"),
        "funding_amount": extract_funding_amount(title, excerpt),
        "company_name": extract_company_name(title, excerpt, md.get("tags", [])),
    }


def fetch_latest():
    scraper = cloudscraper.create_scraper()
    r = scraper.get(API_URL, params={
        "slug": "funding",
        "brand": "yourstory",
        "categoryInfo": "true",
        "limit": LIMIT,
        "offset": 0,
        "requireStoryContent": "false",
    }, timeout=30)
    r.raise_for_status()
    stories = r.json().get("stories", [])
    log.info("Fetched %d stories from API", len(stories))
    return [normalize(s) for s in stories]


def save_to_db(stories):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        log.error("DATABASE_URL not set")
        sys.exit(1)

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS yourstory_funding_articles (
                    id              SERIAL PRIMARY KEY,
                    story_id        BIGINT UNIQUE,
                    title           TEXT NOT NULL,
                    slug            TEXT,
                    url             TEXT,
                    excerpt         TEXT,
                    published_at    TIMESTAMPTZ,
                    image_url       TEXT,
                    author_name     TEXT,
                    category        TEXT,
                    tags            TEXT[],
                    time_to_read    INTEGER,
                    funding_amount  TEXT,
                    company_name    TEXT,
                    scraped_at      TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_yourstory_story_id
                    ON yourstory_funding_articles (story_id);
                CREATE INDEX IF NOT EXISTS idx_yourstory_published_at
                    ON yourstory_funding_articles (published_at);
            """)

            for s in stories:
                cur.execute("""
                    INSERT INTO yourstory_funding_articles
                        (story_id, title, slug, url, excerpt, published_at,
                         image_url, author_name, category, tags, time_to_read,
                         funding_amount, company_name)
                    VALUES
                        (%(story_id)s, %(title)s, %(slug)s, %(url)s, %(excerpt)s,
                         %(published_at)s, %(image_url)s, %(author_name)s,
                         %(category)s, %(tags)s, %(time_to_read)s,
                         %(funding_amount)s, %(company_name)s)
                    ON CONFLICT (story_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        excerpt = EXCLUDED.excerpt,
                        funding_amount = EXCLUDED.funding_amount,
                        company_name = EXCLUDED.company_name,
                        scraped_at = NOW()
                """, s)
        conn.commit()
        log.info("Saved %d stories to database", len(stories))
    finally:
        conn.close()


def main():
    log.info("=== YourStory Funding Scraper (daily) ===")
    stories = fetch_latest()
    if stories:
        save_to_db(stories)
    else:
        log.info("No stories to save")
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
