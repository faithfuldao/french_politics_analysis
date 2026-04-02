"""
Simple HTML scraper for the WikiProject Every Politician pages.

Parses the pre-built HTML tables from pages like:
  https://www.wikidata.org/wiki/Wikidata:WikiProject_every_politician/France/data/Assembly/15th_(bio)

Each row gives us: Q-id, name, and a short bio description.
Dates are stripped from the bio text.

Standalone usage:
    python -m backend.etl.page_scraper
"""

from __future__ import annotations

import logging
import re
import sys
import time

import requests
from bs4 import BeautifulSoup

from backend.models.schema import Person

logger = logging.getLogger(__name__)

# Pages to scrape — add more assembly/senate pages here as needed
PAGES = [
    "https://www.wikidata.org/wiki/Wikidata:WikiProject_every_politician/France/data/Assembly/15th_(bio)",
]

_USER_AGENT = (
    "FrenchPoliticsKnowledgeGraph/1.0 "
    "(educational research project; contact: admin@example.com) "
    "python-requests"
)


def _strip_dates(text: str) -> str:
    """
    Remove date patterns from a bio string.

    Handles:
      - Parenthetical date ranges: (1965–2020), (1965-)
      - "born YYYY" or "né(e) en YYYY"
      - Bare years inside parentheses: (born 15 March 1965)
    """
    # Remove full parenthetical blocks that contain a 4-digit year
    text = re.sub(r"\([^)]*\d{4}[^)]*\)", "", text)
    # Remove "born YYYY" / "né en YYYY" style phrases
    text = re.sub(r"\b(born|né|née)\s+\w+\s+\d{1,2},?\s+\d{4}", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(born|né|née)\s+\d{4}", "", text, flags=re.IGNORECASE)
    # Collapse extra whitespace and strip
    return re.sub(r"\s{2,}", " ", text).strip(" ,;")


def _extract_qid_from_href(href: str) -> str | None:
    """Extract Q-id from a Wikidata /wiki/Q123 href."""
    match = re.search(r"/wiki/(Q\d+)", href)
    return match.group(1) if match else None


def scrape_page(url: str) -> list[Person]:
    """
    Fetch one WikiProject Every Politician page and parse all politician rows.

    Returns a list of Person objects with wikidata_id, name, and description.
    All other Person fields are left as None — they can be enriched later.
    """
    logger.info("Fetching %s ...", url)
    response = requests.get(
        url,
        headers={"User-Agent": _USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # The page contains a wikitable — find it
    table = soup.find("table", class_="wikitable")
    if not table:
        logger.warning("No wikitable found on %s", url)
        return []

    persons: list[Person] = []
    rows = table.find_all("tr")
    skipped = 0

    for row in rows[1:]:  # skip header row
        cells = row.find_all("td")
        if len(cells) < 3:
            skipped += 1
            continue

        # Column layout (0-indexed):
        # 0 = image, 1 = name (linked), 2 = bio description, 3+ = other fields
        name_cell = cells[1]
        bio_cell = cells[2]

        # Extract Q-id from the link in the name cell
        link = name_cell.find("a", href=True)
        if not link:
            skipped += 1
            continue

        qid = _extract_qid_from_href(link["href"])
        if not qid:
            skipped += 1
            continue

        name = link.get_text(strip=True)
        if not name:
            skipped += 1
            continue

        bio_raw = bio_cell.get_text(strip=True)
        bio = _strip_dates(bio_raw) if bio_raw else None

        persons.append(
            Person(
                wikidata_id=qid,
                name=name,
                description=bio or None,
                # All other fields default to None — a missing attribute
                # must never cause this person to be skipped.
            )
        )

    logger.info("  -> %d politicians parsed, %d rows skipped", len(persons), skipped)
    return persons


def scrape_all_pages(pages: list[str] = PAGES, delay: float = 2.0) -> list[Person]:
    """
    Scrape all configured pages and return a deduplicated list of Person objects.
    """
    seen: set[str] = set()
    all_persons: list[Person] = []

    for i, url in enumerate(pages):
        persons = scrape_page(url)
        for p in persons:
            if p.wikidata_id not in seen:
                seen.add(p.wikidata_id)
                all_persons.append(p)
        if i < len(pages) - 1:
            time.sleep(delay)

    logger.info("Total unique politicians scraped: %d", len(all_persons))
    return all_persons


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )

    persons = scrape_all_pages()

    print(f"\n{'-' * 60}")
    print(f"{'Name':<35} {'Q-id':<12} {'Bio'}")
    print(f"{'-' * 60}")
    for p in persons[:30]:
        bio = (p.description or "")[:40]
        print(f"{p.name:<35} {p.wikidata_id:<12} {bio}")
    print(f"{'-' * 60}")
    print(f"Total: {len(persons)} politicians")
