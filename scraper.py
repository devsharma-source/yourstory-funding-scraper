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


def extract_company_name(title):
    clean = re.sub(r"^\[.*?\]\s*", "", title)
    for pat in [
        r"^(.+?)\s+(?:raises?|secures?|gets?|bags?|closes?|receives?|lands?|nabs?|grabs?|clinches?|acquires?)\b",
        r"^(.+?)\s+(?:funding|investment|round)\b",
    ]:
        m = re.search(pat, clean, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            if 2 <= len(name) <= 100:
                return name
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
        "company_name": extract_company_name(title),
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
