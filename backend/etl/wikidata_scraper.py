"""
Wikidata SPARQL scraper for French politicians.

This module contains three public functions:

    fetch_politicians()      -> list[Person]
    fetch_party_memberships() -> list[PartyMembership]
    fetch_mandates()         -> list[Mandate]

Each function queries the Wikidata public SPARQL endpoint with proper
pagination (LIMIT / OFFSET), exponential backoff on failures, and a
polite User-Agent header as required by Wikidata's bot policy.

All dates are normalised to ISO-8601 strings (YYYY-MM-DD) or None.
Wikidata returns dates in the format "+1965-03-15T00:00:00Z"; we strip
the leading sign, the time component, and any precision suffix.

Design note on SPARQL queries
------------------------------
We intentionally use OPTIONAL { } for every nullable property so that
politicians with incomplete records are still returned rather than
silently dropped by an inner join.  This trades result set size for
completeness — appropriate for a knowledge graph that will be enriched
over time.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Optional

import requests

from backend.config import settings
from backend.models.schema import Mandate, Party, PartyMembership, Person

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared HTTP session
# ---------------------------------------------------------------------------

_SESSION: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update(
            {
                # Wikidata requires a descriptive User-Agent.
                # See: https://www.mediawiki.org/wiki/Wikidata_Query_Service/User_Manual#Caching
                "User-Agent": (
                    "FrenchPoliticsKnowledgeGraph/1.0 "
                    "(educational research project; contact: admin@example.com) "
                    "python-requests"
                ),
                "Accept": "application/sparql-results+json",
            }
        )
    return _SESSION


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _parse_date(raw: Optional[str]) -> Optional[str]:
    """
    Convert a Wikidata datetime string to an ISO date string YYYY-MM-DD.

    Wikidata examples:
      "+1965-03-15T00:00:00Z"   -> "1965-03-15"
      "-0044-03-15T00:00:00Z"   -> None  (ancient dates — not useful here)
      ""                        -> None
    """
    if not raw:
        return None
    # Strip leading +/- and extract date portion
    match = re.match(r"[+-]?(\d{4}-\d{2}-\d{2})", raw)
    if not match:
        return None
    date_str = match.group(1)
    # Reject obviously wrong years (Wikidata sometimes has year 1 placeholders)
    year = int(date_str[:4])
    if year < 1800 or year > 2100:
        return None
    return date_str


def _extract_qid(uri: str) -> str:
    """
    Extract the Q-identifier from a Wikidata entity URI.
    e.g. "http://www.wikidata.org/entity/Q1234" -> "Q1234"
    """
    return uri.rsplit("/", 1)[-1]


def _sparql_request(query: str, endpoint: str, retries: int = 5) -> list[dict]:
    """
    Execute a SPARQL SELECT query against `endpoint` and return the bindings list.

    Implements exponential backoff with jitter for rate-limiting (HTTP 429)
    and transient server errors (5xx).

    Parameters
    ----------
    query:
        SPARQL SELECT query string.
    endpoint:
        SPARQL endpoint URL.
    retries:
        Maximum number of retry attempts.

    Returns
    -------
    List of binding dicts, e.g. [{"person": {"type": "uri", "value": "..."}, ...}]
    """
    session = _get_session()
    delay = 2.0  # initial backoff in seconds

    for attempt in range(retries + 1):
        try:
            response = session.get(
                endpoint,
                params={"query": query, "format": "json"},
                timeout=60,
            )

            if response.status_code == 200:
                data = response.json()
                return data["results"]["bindings"]

            if response.status_code == 429 or response.status_code >= 500:
                if attempt < retries:
                    wait = delay * (2 ** attempt)
                    logger.warning(
                        "HTTP %s from SPARQL endpoint. Retrying in %.1fs (attempt %d/%d).",
                        response.status_code,
                        wait,
                        attempt + 1,
                        retries,
                    )
                    time.sleep(wait)
                    continue
                else:
                    response.raise_for_status()

            # Any other 4xx is a query error — don't retry
            response.raise_for_status()

        except requests.exceptions.ConnectionError as exc:
            if attempt < retries:
                wait = delay * (2 ** attempt)
                logger.warning("Connection error: %s. Retrying in %.1fs.", exc, wait)
                time.sleep(wait)
            else:
                raise

    return []


def _paginate_sparql(query_template: str, page_size: int, max_pages: int = 0) -> list[dict]:
    """
    Execute `query_template` with LIMIT/OFFSET pagination and collect all rows.

    The template must contain exactly two format placeholders: {limit} and {offset}.

    Parameters
    ----------
    query_template:
        A SPARQL query string with {limit} and {offset} placeholders.
    page_size:
        Number of results per page.
    max_pages:
        0 means fetch all pages; N > 0 stops after N pages (useful for testing).
    """
    endpoint = settings.wikidata_sparql_endpoint
    all_rows: list[dict] = []
    page = 0

    while True:
        offset = page * page_size
        query = query_template.format(limit=page_size, offset=offset)

        logger.info("Fetching SPARQL page %d (offset=%d, limit=%d) ...", page + 1, offset, page_size)
        rows = _sparql_request(query, endpoint, retries=settings.etl_max_retries)

        all_rows.extend(rows)
        logger.info("  -> Got %d rows (total so far: %d)", len(rows), len(all_rows))

        if len(rows) < page_size:
            # Last page — fewer results than page_size means we've exhausted the dataset
            break

        page += 1
        if max_pages > 0 and page >= max_pages:
            logger.info("Reached max_pages limit (%d). Stopping pagination.", max_pages)
            break

        # Polite delay between requests
        time.sleep(settings.etl_request_delay)

    return all_rows


# ---------------------------------------------------------------------------
# Query 1: Base politician data
# ---------------------------------------------------------------------------

_POLITICIANS_QUERY = """
SELECT DISTINCT
  ?person
  ?personLabel
  ?dateOfBirth
  ?dateOfDeath
  ?genderLabel
  ?wikipediaFR
  ?image
WHERE {{
  # Must be an instance of human
  ?person wdt:P31 wd:Q5 .

  # Must have held a political position in France
  ?person p:P39 ?posStmt .
  ?posStmt ps:P39 ?pos .

  # The position must be a French political position (country = France)
  ?pos wdt:P17 wd:Q142 .

  OPTIONAL {{ ?person wdt:P569 ?dateOfBirth . }}
  OPTIONAL {{ ?person wdt:P570 ?dateOfDeath . }}
  OPTIONAL {{ ?person wdt:P21 ?gender . }}
  OPTIONAL {{
    ?wikipediaFR schema:about ?person ;
                 schema:inLanguage "fr" ;
                 schema:isPartOf <https://fr.wikipedia.org/> .
  }}
  OPTIONAL {{ ?person wdt:P18 ?image . }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "fr,en" .
  }}
}}
ORDER BY ?person
LIMIT {limit}
OFFSET {offset}
"""


def fetch_politicians(
    page_size: Optional[int] = None,
    max_pages: int = 0,
) -> list[Person]:
    """
    Fetch base person records for French politicians from Wikidata.

    Parameters
    ----------
    page_size:
        Results per SPARQL page.  Defaults to settings.etl_page_size.
    max_pages:
        0 = all pages.  Set to a small number for testing (e.g. 1 = 200 politicians).

    Returns
    -------
    Deduplicated list of Person instances.
    """
    if page_size is None:
        page_size = settings.etl_page_size

    rows = _paginate_sparql(_POLITICIANS_QUERY, page_size=page_size, max_pages=max_pages)

    seen: set[str] = set()
    persons: list[Person] = []

    for row in rows:
        try:
            qid = _extract_qid(row["person"]["value"])
            if qid in seen:
                continue
            seen.add(qid)

            gender_raw = row.get("genderLabel", {}).get("value")
            gender: Optional[str] = None
            if gender_raw:
                g = gender_raw.lower()
                if "female" in g or "femme" in g or "féminin" in g:
                    gender = "female"
                elif "male" in g or "masculin" in g or "homme" in g:
                    gender = "male"
                else:
                    gender = "other"

            persons.append(
                Person(
                    wikidata_id=qid,
                    name=row.get("personLabel", {}).get("value", qid),
                    date_of_birth=_parse_date(row.get("dateOfBirth", {}).get("value")),
                    date_of_death=_parse_date(row.get("dateOfDeath", {}).get("value")),
                    gender=gender,
                    wikipedia_fr_url=row.get("wikipediaFR", {}).get("value"),
                    image_url=row.get("image", {}).get("value"),
                )
            )
        except Exception as exc:
            logger.warning("Skipping malformed row: %s — %s", row, exc)

    logger.info("fetch_politicians(): %d unique persons fetched.", len(persons))
    return persons


# ---------------------------------------------------------------------------
# Query 2: Party memberships
# ---------------------------------------------------------------------------

_MEMBERSHIPS_QUERY = """
SELECT DISTINCT
  ?person
  ?party
  ?partyLabel
  ?partyShortName
  ?startDate
  ?endDate
WHERE {{
  # Person must hold a French political position (same filter as politicians query)
  ?person wdt:P31 wd:Q5 .
  ?person p:P39 ?posStmt .
  ?posStmt ps:P39 ?pos .
  ?pos wdt:P17 wd:Q142 .

  # Party membership statement
  ?person p:P102 ?memberStmt .
  ?memberStmt ps:P102 ?party .

  OPTIONAL {{ ?memberStmt pq:P580 ?startDate . }}
  OPTIONAL {{ ?memberStmt pq:P582 ?endDate . }}
  OPTIONAL {{ ?party wdt:P1813 ?partyShortName . }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "fr,en" .
  }}
}}
ORDER BY ?person ?party
LIMIT {limit}
OFFSET {offset}
"""


def fetch_party_memberships(
    page_size: Optional[int] = None,
    max_pages: int = 0,
) -> tuple[list[PartyMembership], list[Party]]:
    """
    Fetch party membership records and derive Party nodes.

    Returns a tuple of (memberships, parties) so the caller can upsert both.

    Parameters
    ----------
    page_size:
        Results per page.
    max_pages:
        0 = all pages.
    """
    if page_size is None:
        page_size = settings.etl_page_size

    rows = _paginate_sparql(_MEMBERSHIPS_QUERY, page_size=page_size, max_pages=max_pages)

    memberships: list[PartyMembership] = []
    parties_by_id: dict[str, Party] = {}

    for row in rows:
        try:
            person_qid = _extract_qid(row["person"]["value"])
            party_qid = _extract_qid(row["party"]["value"])

            # Collect Party data opportunistically
            if party_qid not in parties_by_id:
                party_name = row.get("partyLabel", {}).get("value", party_qid)
                # Skip parties whose label is just the Q-id (unresolved)
                if party_name == party_qid:
                    party_name = f"Parti {party_qid}"
                parties_by_id[party_qid] = Party(
                    wikidata_id=party_qid,
                    name=party_name,
                    short_name=row.get("partyShortName", {}).get("value"),
                )

            start = _parse_date(row.get("startDate", {}).get("value"))
            end = _parse_date(row.get("endDate", {}).get("value"))

            # A membership is "current" if there is no end date recorded
            is_current = end is None

            memberships.append(
                PartyMembership(
                    person_wikidata_id=person_qid,
                    party_wikidata_id=party_qid,
                    from_date=start,
                    to_date=end,
                    is_current=is_current,
                )
            )
        except Exception as exc:
            logger.warning("Skipping malformed membership row: %s — %s", row, exc)

    parties = list(parties_by_id.values())
    logger.info(
        "fetch_party_memberships(): %d memberships, %d unique parties.",
        len(memberships),
        len(parties),
    )
    return memberships, parties


# ---------------------------------------------------------------------------
# Query 3: Mandates / political roles
# ---------------------------------------------------------------------------

# Mapping from Wikidata position Q-ids to canonical French role names.
# We use this to normalise the many specific Wikidata positions into a
# smaller set of Role nodes in our graph.
POSITION_ROLE_MAP: dict[str, str] = {
    "Q3291418": "Député",                    # member of the National Assembly
    "Q3918957": "Sénateur",                  # member of the French Senate
    "Q191954":  "Président de la République",
    "Q191958":  "Premier ministre",
    "Q17375": "Ministre",                    # government minister (generic)
    "Q382617":  "Maire",                     # mayor
    "Q483388":  "Président de région",
    "Q1021645": "Conseiller régional",
    "Q1307288": "Conseiller départemental",
    "Q2311456": "Député européen",           # MEP
    "Q27169":   "Secrétaire d'État",
    "Q2285706": "Préfet",
}

_MANDATES_QUERY = """
SELECT DISTINCT
  ?person
  ?position
  ?positionLabel
  ?startDate
  ?endDate
  ?constituency
  ?constituencyLabel
WHERE {{
  # Person must be a French politician
  ?person wdt:P31 wd:Q5 .
  ?person p:P39 ?posStmt .
  ?posStmt ps:P39 ?position .

  # Position must be tied to France
  ?position wdt:P17 wd:Q142 .

  OPTIONAL {{ ?posStmt pq:P580 ?startDate . }}
  OPTIONAL {{ ?posStmt pq:P582 ?endDate . }}
  OPTIONAL {{ ?posStmt pq:P768 ?constituency . }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "fr,en" .
  }}
}}
ORDER BY ?person ?position
LIMIT {limit}
OFFSET {offset}
"""


def _normalise_role_name(position_qid: str, position_label: str) -> str:
    """
    Map a Wikidata position Q-id to a canonical role name.

    Falls back to the French label from Wikidata if the Q-id is not in our map.
    Strips trailing geographic qualifiers like "(Paris)" from labels.
    """
    if position_qid in POSITION_ROLE_MAP:
        return POSITION_ROLE_MAP[position_qid]

    # Clean up the label: remove parenthetical suffixes
    label = re.sub(r"\s*\([^)]*\)\s*$", "", position_label).strip()
    return label if label else position_qid


def fetch_mandates(
    page_size: Optional[int] = None,
    max_pages: int = 0,
) -> list[Mandate]:
    """
    Fetch political mandates (roles held) for French politicians.

    Parameters
    ----------
    page_size:
        Results per page.
    max_pages:
        0 = all pages.

    Returns
    -------
    List of Mandate instances.
    """
    if page_size is None:
        page_size = settings.etl_page_size

    rows = _paginate_sparql(_MANDATES_QUERY, page_size=page_size, max_pages=max_pages)

    mandates: list[Mandate] = []

    for row in rows:
        try:
            person_qid = _extract_qid(row["person"]["value"])
            position_qid = _extract_qid(row["position"]["value"])
            position_label = row.get("positionLabel", {}).get("value", position_qid)

            role_name = _normalise_role_name(position_qid, position_label)

            start = _parse_date(row.get("startDate", {}).get("value"))
            end = _parse_date(row.get("endDate", {}).get("value"))
            is_current = end is None

            constituency = row.get("constituencyLabel", {}).get("value")

            mandates.append(
                Mandate(
                    person_wikidata_id=person_qid,
                    role_name=role_name,
                    from_date=start,
                    to_date=end,
                    is_current=is_current,
                    constituency=constituency,
                    wikidata_position_id=position_qid,
                )
            )
        except Exception as exc:
            logger.warning("Skipping malformed mandate row: %s — %s", row, exc)

    logger.info("fetch_mandates(): %d mandates fetched.", len(mandates))
    return mandates
