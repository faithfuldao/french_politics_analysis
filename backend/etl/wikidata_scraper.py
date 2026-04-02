"""
Wikidata two-phase scraper for French politicians.

Phase 1 — ID harvest
---------------------
Small, targeted SPARQL queries per assembly session, senate, and executive
position fetch only ?item (Q-ids).  Each query is ultra-fast: no labels,
no OPTIONAL properties, no country joins.  These queries never 504.

Phase 2 — Entity fetch
-----------------------
Batches of up to 50 Q-ids are sent to the Wikidata ``wbgetentities`` REST
API, which returns the full JSON entity blob per politician.  Properties
are extracted from the claim arrays rather than from SPARQL bindings.

Public interface (unchanged from the previous implementation)
--------------------------------------------------------------
    fetch_politicians(page_size=None, max_pages=0) -> list[Person]
    fetch_party_memberships(page_size=None, max_pages=0) -> tuple[list[PartyMembership], list[Party]]
    fetch_mandates(page_size=None, max_pages=0) -> list[Mandate]

The ``page_size`` parameter is repurposed as the wbgetentities batch size
(default 50, which is the API maximum).  ``max_pages`` is repurposed as the
maximum number of entity batches to process — set to 1 for a quick smoke
test covering only the first 50 entities.

All three public functions share a single entity fetch pass via a
module-level cache so the HTTP work is only done once per process.

All dates are normalised to ISO-8601 strings (YYYY-MM-DD) or None.
Wikidata claim dates arrive as "+1965-03-15T00:00:00Z"; the same
``_parse_date()`` helper strips the leading sign and time component.
"""

from __future__ import annotations

import logging
import re
import time
import urllib.parse
from typing import Optional

import requests

from backend.config import settings
from backend.models.schema import Mandate, Party, PartyMembership, Person

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared HTTP session
# ---------------------------------------------------------------------------

_SESSION: Optional[requests.Session] = None

_USER_AGENT = (
    "FrenchPoliticsKnowledgeGraph/1.0 "
    "(educational research project; contact: admin@example.com) "
    "python-requests"
)


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update(
            {
                # Wikidata requires a descriptive User-Agent.
                # See: https://www.mediawiki.org/wiki/Wikidata_Query_Service/User_Manual#Caching
                "User-Agent": _USER_AGENT,
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


def _claim_time(entity: dict, prop: str) -> Optional[str]:
    """
    Extract the first time value from a claim array.

    Returns the parsed ISO date string or None.
    """
    claims = entity.get("claims", {}).get(prop, [])
    if not claims:
        return None
    try:
        raw = claims[0]["mainsnak"]["datavalue"]["value"]["time"]
        return _parse_date(raw)
    except (KeyError, IndexError, TypeError):
        return None


def _claim_item_id(entity: dict, prop: str) -> Optional[str]:
    """
    Extract the first item (Q-id) value from a claim array.
    """
    claims = entity.get("claims", {}).get(prop, [])
    if not claims:
        return None
    try:
        return claims[0]["mainsnak"]["datavalue"]["value"]["id"]
    except (KeyError, IndexError, TypeError):
        return None


def _claim_string(entity: dict, prop: str) -> Optional[str]:
    """
    Extract the first string value from a claim array.
    """
    claims = entity.get("claims", {}).get(prop, [])
    if not claims:
        return None
    try:
        return claims[0]["mainsnak"]["datavalue"]["value"]
    except (KeyError, IndexError, TypeError):
        return None


def _qualifier_time(statement: dict, prop: str) -> Optional[str]:
    """
    Extract a time qualifier from a SPARQL-style Wikidata statement dict.
    """
    qualifiers = statement.get("qualifiers", {}).get(prop, [])
    if not qualifiers:
        return None
    try:
        raw = qualifiers[0]["datavalue"]["value"]["time"]
        return _parse_date(raw)
    except (KeyError, IndexError, TypeError):
        return None


def _qualifier_item_id(statement: dict, prop: str) -> Optional[str]:
    """
    Extract an item (Q-id) qualifier from a statement dict.
    """
    qualifiers = statement.get("qualifiers", {}).get(prop, [])
    if not qualifiers:
        return None
    try:
        return qualifiers[0]["datavalue"]["value"]["id"]
    except (KeyError, IndexError, TypeError):
        return None


def _entity_label(entity: dict) -> str:
    """
    Return the French label for an entity, falling back to English, then Q-id.
    """
    qid = entity.get("id", "")
    labels = entity.get("labels", {})
    fr = labels.get("fr", {}).get("value")
    if fr:
        return fr
    en = labels.get("en", {}).get("value")
    if en:
        return en
    return qid


# ---------------------------------------------------------------------------
# Phase 1: SPARQL ID harvest
# ---------------------------------------------------------------------------

# Assembly sessions to harvest (position Q-id -> human-readable label)
ASSEMBLY_SESSIONS: dict[str, str] = {
    "Q24939798": "15th",   # 2017–2022
    "Q112567407": "16th",  # 2022–present
}

# Wikidata Q-ids referenced in the harvest queries
_QIDS = {
    "depute":   "Q3044918",  # member of French National Assembly (Député)
    "senateur": "Q3918957",  # member of French Senate
}

# Executive positions: Président de la République, Premier ministre,
# Ministre, Secrétaire d'État
_EXECUTIVE_POSITIONS = "wd:Q191954 wd:Q191958 wd:Q17375 wd:Q27169"

_SPARQL_ENDPOINT = settings.wikidata_sparql_endpoint


def _sparql_request(query: str, endpoint: str, retries: int = 5) -> list[dict]:
    """
    Execute a SPARQL SELECT query against ``endpoint`` and return the bindings.

    Implements exponential backoff for HTTP 429 and 5xx responses.
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


def _run_harvest_query(sparql: str) -> set[str]:
    """
    Run a single ID-harvest SPARQL query and return the set of Q-ids found.

    The query must return a single variable ``?item`` bound to Wikidata entity
    URIs.
    """
    bindings = _sparql_request(sparql, _SPARQL_ENDPOINT, retries=settings.etl_max_retries)
    qids: set[str] = set()
    for row in bindings:
        try:
            uri = row["item"]["value"]
            qids.add(_extract_qid(uri))
        except KeyError:
            pass
    return qids


def _harvest_qids() -> set[str]:
    """
    Run all small SPARQL harvest queries and collect unique Q-ids.

    Covers:
    - National Assembly deputies per session (15th and 16th legislatures)
    - Senate members (continuous mandate, no session qualifier)
    - Executive officials (presidents, prime ministers, ministers,
      secretaries of state)
    """
    all_qids: set[str] = set()

    # 1. National Assembly deputies — one query per session
    for session_qid, label in ASSEMBLY_SESSIONS.items():
        query = (
            "SELECT ?item WHERE { "
            f"?item p:P39 [ ps:P39 wd:{_QIDS['depute']} ; pq:P2937 wd:{session_qid}] "
            "}"
        )
        logger.info("Harvesting Assembly deputies — %s legislature (session %s)...", label, session_qid)
        found = _run_harvest_query(query)
        logger.info("  -> %d Q-ids", len(found))
        all_qids.update(found)
        time.sleep(1)

    # 2. Senate members — no session qualifier (continuous mandates)
    senate_query = (
        "SELECT ?item WHERE { "
        f"?item p:P39 [ ps:P39 wd:{_QIDS['senateur']} ] "
        "}"
    )
    logger.info("Harvesting Senate members...")
    found = _run_harvest_query(senate_query)
    logger.info("  -> %d Q-ids", len(found))
    all_qids.update(found)
    time.sleep(1)

    # 3. Executive positions (presidents, PMs, ministers, secretaries of state)
    exec_query = (
        f"SELECT ?item WHERE {{ VALUES ?pos {{ {_EXECUTIVE_POSITIONS} }} "
        "?item wdt:P39 ?pos . }"
    )
    logger.info("Harvesting executive officials...")
    found = _run_harvest_query(exec_query)
    logger.info("  -> %d Q-ids", len(found))
    all_qids.update(found)

    logger.info("Total unique Q-ids harvested: %d", len(all_qids))
    return all_qids


# ---------------------------------------------------------------------------
# Phase 2: wbgetentities batch fetch
# ---------------------------------------------------------------------------

_WBENTITIES_URL = "https://www.wikidata.org/w/api.php"
_MAX_IDS_PER_REQUEST = 50


def _fetch_entities_batch(qids: list[str], retries: int = 5) -> dict:
    """
    Call the Wikidata ``wbgetentities`` API for up to 50 Q-ids.

    Returns the raw ``entities`` dict keyed by Q-id.
    On failure returns an empty dict (after exhausting retries).
    """
    session = _get_session()
    params = {
        "action": "wbgetentities",
        "ids": "|".join(qids),
        "format": "json",
        "languages": "fr|en",
        "props": "labels|claims|sitelinks",
    }
    delay = 2.0

    for attempt in range(retries + 1):
        try:
            response = session.get(
                _WBENTITIES_URL,
                params=params,
                timeout=60,
                headers={"Accept": "application/json"},
            )

            if response.status_code == 200:
                data = response.json()
                return data.get("entities", {})

            if response.status_code == 429 or response.status_code >= 500:
                if attempt < retries:
                    wait = delay * (2 ** attempt)
                    logger.warning(
                        "HTTP %s from wbgetentities. Retrying in %.1fs (attempt %d/%d).",
                        response.status_code,
                        wait,
                        attempt + 1,
                        retries,
                    )
                    time.sleep(wait)
                    continue
                else:
                    logger.error(
                        "wbgetentities failed after %d retries for batch: %s",
                        retries,
                        qids[:5],
                    )
                    return {}

            response.raise_for_status()

        except requests.exceptions.ConnectionError as exc:
            if attempt < retries:
                wait = delay * (2 ** attempt)
                logger.warning("Connection error: %s. Retrying in %.1fs.", exc, wait)
                time.sleep(wait)
            else:
                logger.error("Connection failed for wbgetentities batch: %s", exc)
                return {}

    return {}


def _fetch_all_entities(
    qids: set[str],
    batch_size: int = _MAX_IDS_PER_REQUEST,
    max_batches: int = 0,
) -> dict:
    """
    Fetch all entity data for the given Q-ids in batches.

    Parameters
    ----------
    qids:
        Set of Wikidata Q-identifiers.
    batch_size:
        Number of Q-ids per wbgetentities request (max 50).
    max_batches:
        0 = fetch all batches.  N > 0 stops after N batches (smoke testing).

    Returns
    -------
    Merged ``entities`` dict keyed by Q-id.
    """
    batch_size = min(batch_size, _MAX_IDS_PER_REQUEST)
    qid_list = sorted(qids)  # deterministic order
    all_entities: dict = {}
    total_batches = (len(qid_list) + batch_size - 1) // batch_size

    logger.info(
        "Fetching %d entities in %d batches of %d...",
        len(qid_list),
        total_batches,
        batch_size,
    )

    for batch_num, i in enumerate(range(0, len(qid_list), batch_size)):
        if max_batches > 0 and batch_num >= max_batches:
            logger.info("Reached max_batches limit (%d). Stopping entity fetch.", max_batches)
            break

        batch = qid_list[i : i + batch_size]
        logger.info(
            "  Batch %d/%d — fetching %d entities (%s … %s)...",
            batch_num + 1,
            total_batches,
            len(batch),
            batch[0],
            batch[-1],
        )

        entities = _fetch_entities_batch(batch, retries=settings.etl_max_retries)
        all_entities.update(entities)

        logger.info("  -> %d entities received (running total: %d)", len(entities), len(all_entities))

        if i + batch_size < len(qid_list):
            # Polite delay between requests — 1 to 3 seconds
            time.sleep(max(1.0, min(3.0, settings.etl_request_delay)))

    return all_entities


# ---------------------------------------------------------------------------
# Module-level entity cache — populated on first call, shared across all
# three public functions within the same process run.
# ---------------------------------------------------------------------------

_ENTITY_CACHE: Optional[dict] = None
_CACHE_BATCH_SIZE: int = _MAX_IDS_PER_REQUEST
_CACHE_MAX_BATCHES: int = 0


def _get_entities(batch_size: int, max_batches: int) -> dict:
    """
    Return the cached entity dict, or populate it on first call.

    If called multiple times with different parameters (e.g. during testing),
    the cache from the first call is reused.  The parameters only take effect
    on the very first invocation.
    """
    global _ENTITY_CACHE, _CACHE_BATCH_SIZE, _CACHE_MAX_BATCHES

    if _ENTITY_CACHE is not None:
        return _ENTITY_CACHE

    _CACHE_BATCH_SIZE = batch_size
    _CACHE_MAX_BATCHES = max_batches

    logger.info("--- Phase 1: Harvesting Q-ids via SPARQL ---")
    qids = _harvest_qids()

    logger.info("--- Phase 2: Fetching entity data via wbgetentities ---")
    _ENTITY_CACHE = _fetch_all_entities(qids, batch_size=batch_size, max_batches=max_batches)

    return _ENTITY_CACHE


# ---------------------------------------------------------------------------
# Mapping from Wikidata position Q-ids to canonical French role names.
# ---------------------------------------------------------------------------

POSITION_ROLE_MAP: dict[str, str] = {
    "Q3291418": "Député",                    # member of the National Assembly (generic)
    "Q3044918": "Député",                    # member of the National Assembly (via session query)
    "Q3918957": "Sénateur",                  # member of the French Senate
    "Q191954":  "Président de la République",
    "Q191958":  "Premier ministre",
    "Q17375":   "Ministre",                  # government minister (generic)
    "Q382617":  "Maire",                     # mayor
    "Q483388":  "Président de région",
    "Q1021645": "Conseiller régional",
    "Q1307288": "Conseiller départemental",
    "Q2311456": "Député européen",           # MEP
    "Q27169":   "Secrétaire d'État",
    "Q2285706": "Préfet",
}

# Gender Q-id mapping
_GENDER_MAP: dict[str, str] = {
    "Q6581072": "female",
    "Q6581097": "male",
}

# ---------------------------------------------------------------------------
# Parse helpers
# ---------------------------------------------------------------------------

def _normalise_role_name(position_qid: str, position_label: str) -> str:
    """
    Map a Wikidata position Q-id to a canonical role name.

    Falls back to the French label from Wikidata if the Q-id is not in the map.
    Strips trailing geographic qualifiers like "(Paris)" from labels.
    """
    if position_qid in POSITION_ROLE_MAP:
        return POSITION_ROLE_MAP[position_qid]

    # Clean up the label: remove parenthetical suffixes
    label = re.sub(r"\s*\([^)]*\)\s*$", "", position_label).strip()
    return label if label else position_qid


def _image_url_from_filename(filename: Optional[str]) -> Optional[str]:
    """
    Construct a Wikimedia Commons Special:FilePath URL from an image filename.

    The filename comes from the P18 claim string value, e.g. "Jean Dupont.jpg".
    We URL-encode spaces and special characters using the same convention as
    Wikimedia (spaces -> underscores in the canonical form, but Special:FilePath
    accepts the raw filename too via percent-encoding).
    """
    if not filename:
        return None
    # Wikimedia normalises spaces to underscores in filenames
    encoded = urllib.parse.quote(filename.replace(" ", "_"), safe="")
    return f"https://commons.wikimedia.org/wiki/Special:FilePath/{encoded}"


def _wikipedia_fr_url(entity: dict) -> Optional[str]:
    """
    Extract the French Wikipedia URL from sitelinks.
    """
    try:
        return entity["sitelinks"]["frwiki"]["url"]
    except KeyError:
        return None


# ---------------------------------------------------------------------------
# Entity-level parsers — one per domain object
# ---------------------------------------------------------------------------

def _parse_person(qid: str, entity: dict) -> Optional[Person]:
    """
    Parse a wbgetentities entity blob into a Person model.

    Returns None if the entity is a redirect or has no usable label.
    """
    # Wikidata marks missing or redirected entities
    if entity.get("missing") == "" or entity.get("redirects"):
        return None

    name = _entity_label(entity)
    # If the label is just the Q-id, the entity likely has no useful label
    if name == qid:
        logger.debug("Skipping %s — no label found.", qid)
        return None

    gender_qid = _claim_item_id(entity, "P21")
    gender = _GENDER_MAP.get(gender_qid) if gender_qid else None

    image_filename = _claim_string(entity, "P18")
    image_url = _image_url_from_filename(image_filename)

    return Person(
        wikidata_id=qid,
        name=name,
        date_of_birth=_claim_time(entity, "P569"),
        date_of_death=_claim_time(entity, "P570"),
        gender=gender,
        wikipedia_fr_url=_wikipedia_fr_url(entity),
        image_url=image_url,
    )


def _parse_party_memberships(
    qid: str,
    entity: dict,
    parties_by_id: dict[str, Party],
) -> list[PartyMembership]:
    """
    Parse all P102 (party membership) claims for one entity.

    Populates ``parties_by_id`` in-place as a side effect.
    Returns a list of PartyMembership objects (may be empty).
    """
    memberships: list[PartyMembership] = []
    p102_claims = entity.get("claims", {}).get("P102", [])

    for stmt in p102_claims:
        try:
            mainsnak = stmt.get("mainsnak", {})
            if mainsnak.get("snaktype") != "value":
                continue

            party_qid = mainsnak["datavalue"]["value"]["id"]

            # Collect Party node data from the claim — we only have the Q-id
            # here; the label will be fetched if we encounter the party entity
            # directly, otherwise we leave a placeholder.
            if party_qid not in parties_by_id:
                parties_by_id[party_qid] = Party(
                    wikidata_id=party_qid,
                    name=f"Parti {party_qid}",  # placeholder — enriched later
                )

            start = _qualifier_time(stmt, "P580")
            end = _qualifier_time(stmt, "P582")
            is_current = end is None

            memberships.append(
                PartyMembership(
                    person_wikidata_id=qid,
                    party_wikidata_id=party_qid,
                    from_date=start,
                    to_date=end,
                    is_current=is_current,
                )
            )
        except Exception as exc:
            logger.debug("Skipping malformed P102 claim for %s: %s", qid, exc)

    return memberships


def _parse_mandates(qid: str, entity: dict) -> list[Mandate]:
    """
    Parse all P39 (position held) claims for one entity.

    Only positions present in POSITION_ROLE_MAP are included.  Positions not
    in the map are also included but receive a label-derived role name.
    """
    mandates: list[Mandate] = []
    p39_claims = entity.get("claims", {}).get("P39", [])

    for stmt in p39_claims:
        try:
            mainsnak = stmt.get("mainsnak", {})
            if mainsnak.get("snaktype") != "value":
                continue

            position_qid = mainsnak["datavalue"]["value"]["id"]
            # Only keep positions that map to a known role
            if position_qid not in POSITION_ROLE_MAP:
                continue

            role_name = POSITION_ROLE_MAP[position_qid]
            start = _qualifier_time(stmt, "P580")
            end = _qualifier_time(stmt, "P582")
            is_current = end is None

            # P768 = electoral district / constituency
            constituency_qid = _qualifier_item_id(stmt, "P768")
            # We store the Q-id as a string — the loader will use it as-is.
            # A future enrichment pass can resolve it to a name.
            constituency: Optional[str] = constituency_qid

            mandates.append(
                Mandate(
                    person_wikidata_id=qid,
                    role_name=role_name,
                    from_date=start,
                    to_date=end,
                    is_current=is_current,
                    constituency=constituency,
                    wikidata_position_id=position_qid,
                )
            )
        except Exception as exc:
            logger.debug("Skipping malformed P39 claim for %s: %s", qid, exc)

    return mandates


def _enrich_party_labels(parties_by_id: dict[str, Party], entities: dict) -> None:
    """
    Replace placeholder Party names with real labels wherever the party entity
    appears in the fetched entity set.

    This is a best-effort pass — parties that were not fetched (because they are
    not politicians themselves) keep their "Parti Q..." placeholder name.
    """
    for party_qid, party in parties_by_id.items():
        if party_qid in entities:
            entity = entities[party_qid]
            label = _entity_label(entity)
            if label and label != party_qid:
                # Build an enriched Party with any extra fields we can extract
                parties_by_id[party_qid] = Party(
                    wikidata_id=party_qid,
                    name=label,
                    short_name=party.short_name,
                    founded_date=_claim_time(entity, "P571"),
                    dissolved_date=_claim_time(entity, "P576"),
                )


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def fetch_politicians(
    page_size: Optional[int] = None,
    max_pages: int = 0,
) -> list[Person]:
    """
    Fetch base person records for French politicians from Wikidata.

    Parameters
    ----------
    page_size:
        wbgetentities batch size (max 50, default 50).
    max_pages:
        Maximum number of entity batches to process.  0 = all batches.
        Set to 1 for a smoke test covering the first 50 entities.

    Returns
    -------
    Deduplicated list of Person instances, one per unique Q-id.
    """
    batch_size = min(page_size or _MAX_IDS_PER_REQUEST, _MAX_IDS_PER_REQUEST)
    entities = _get_entities(batch_size=batch_size, max_batches=max_pages)

    persons: list[Person] = []
    for qid, entity in entities.items():
        try:
            person = _parse_person(qid, entity)
            if person is not None:
                persons.append(person)
        except Exception as exc:
            logger.warning("Skipping entity %s during person parse: %s", qid, exc)

    logger.info("fetch_politicians(): %d unique persons parsed.", len(persons))
    return persons


def fetch_party_memberships(
    page_size: Optional[int] = None,
    max_pages: int = 0,
) -> tuple[list[PartyMembership], list[Party]]:
    """
    Fetch party membership records and derive Party nodes.

    Parameters
    ----------
    page_size:
        wbgetentities batch size (max 50, default 50).
    max_pages:
        Maximum number of entity batches to process.  0 = all batches.

    Returns
    -------
    Tuple of (memberships, parties) for the caller to upsert both.
    """
    batch_size = min(page_size or _MAX_IDS_PER_REQUEST, _MAX_IDS_PER_REQUEST)
    entities = _get_entities(batch_size=batch_size, max_batches=max_pages)

    memberships: list[PartyMembership] = []
    parties_by_id: dict[str, Party] = {}

    for qid, entity in entities.items():
        if entity.get("missing") == "" or entity.get("redirects"):
            continue
        try:
            ms = _parse_party_memberships(qid, entity, parties_by_id)
            memberships.extend(ms)
        except Exception as exc:
            logger.warning("Skipping entity %s during membership parse: %s", qid, exc)

    # Best-effort label enrichment for parties that appear in the entity set
    _enrich_party_labels(parties_by_id, entities)

    parties = list(parties_by_id.values())
    logger.info(
        "fetch_party_memberships(): %d memberships, %d unique parties parsed.",
        len(memberships),
        len(parties),
    )
    return memberships, parties


def fetch_mandates(
    page_size: Optional[int] = None,
    max_pages: int = 0,
) -> list[Mandate]:
    """
    Fetch political mandates (roles held) for French politicians.

    Parameters
    ----------
    page_size:
        wbgetentities batch size (max 50, default 50).
    max_pages:
        Maximum number of entity batches to process.  0 = all batches.

    Returns
    -------
    List of Mandate instances.
    """
    batch_size = min(page_size or _MAX_IDS_PER_REQUEST, _MAX_IDS_PER_REQUEST)
    entities = _get_entities(batch_size=batch_size, max_batches=max_pages)

    mandates: list[Mandate] = []

    for qid, entity in entities.items():
        if entity.get("missing") == "" or entity.get("redirects"):
            continue
        try:
            ms = _parse_mandates(qid, entity)
            mandates.extend(ms)
        except Exception as exc:
            logger.warning("Skipping entity %s during mandate parse: %s", qid, exc)

    logger.info("fetch_mandates(): %d mandates parsed.", len(mandates))
    return mandates
