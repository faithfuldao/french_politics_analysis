"""
Neo4j ETL loader for the French Politics Knowledge Graph.

This module transforms scraped Wikidata records into graph nodes and
relationships using the Neo4j Python driver.  All Cypher statements use
$param parameterised syntax — never string interpolation.

Every upsert function uses MERGE so the pipeline is fully idempotent:
re-running it will update properties without creating duplicates.

Standalone execution
--------------------
    python -m backend.etl.neo4j_loader

This will execute the full ETL pipeline: scrape Wikidata → load into Neo4j.
"""

from __future__ import annotations

import logging
import sys
from typing import Optional

from neo4j import ManagedTransaction

from backend.db.neo4j_client import close_driver, get_driver, run_constraints
from backend.etl.wikidata_scraper import (
    fetch_mandates,
    fetch_party_memberships,
    fetch_politicians,
)
from backend.models.schema import Mandate, Party, PartyMembership, Person, Role

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Person upserts
# ---------------------------------------------------------------------------

_UPSERT_PERSON_QUERY = """
MERGE (p:Person {wikidata_id: $wikidata_id})
SET
  p.name            = $name,
  p.date_of_birth   = $date_of_birth,
  p.date_of_death   = $date_of_death,
  p.gender          = $gender,
  p.wikipedia_fr_url = $wikipedia_fr_url,
  p.image_url       = $image_url,
  p.updated_at      = datetime()
RETURN p.wikidata_id AS wikidata_id
"""


def upsert_person(tx: ManagedTransaction, person: Person) -> None:
    """
    MERGE a Person node on wikidata_id and SET all scalar properties.

    Parameters
    ----------
    tx:
        A Neo4j ManagedTransaction (passed in by execute_write).
    person:
        Validated Person Pydantic model.
    """
    tx.run(
        _UPSERT_PERSON_QUERY,
        wikidata_id=person.wikidata_id,
        name=person.name,
        date_of_birth=person.date_of_birth,
        date_of_death=person.date_of_death,
        gender=person.gender,
        wikipedia_fr_url=person.wikipedia_fr_url,
        image_url=person.image_url,
    )


# ---------------------------------------------------------------------------
# Party upserts
# ---------------------------------------------------------------------------

_UPSERT_PARTY_QUERY = """
MERGE (p:Party {wikidata_id: $wikidata_id})
SET
  p.name           = $name,
  p.short_name     = $short_name,
  p.founded_date   = $founded_date,
  p.dissolved_date = $dissolved_date,
  p.ideology       = $ideology,
  p.updated_at     = datetime()
RETURN p.wikidata_id AS wikidata_id
"""


def upsert_party(tx: ManagedTransaction, party: Party) -> None:
    """
    MERGE a Party node on wikidata_id and SET all scalar properties.
    """
    tx.run(
        _UPSERT_PARTY_QUERY,
        wikidata_id=party.wikidata_id,
        name=party.name,
        short_name=party.short_name,
        founded_date=party.founded_date,
        dissolved_date=party.dissolved_date,
        ideology=party.ideology,
    )


# ---------------------------------------------------------------------------
# Role upserts
# ---------------------------------------------------------------------------

_UPSERT_ROLE_QUERY = """
MERGE (r:Role {name: $name})
SET
  r.wikidata_id = $wikidata_id,
  r.level       = $level,
  r.updated_at  = datetime()
RETURN r.name AS name
"""


def upsert_role(tx: ManagedTransaction, role: Role) -> None:
    """
    MERGE a Role node on its canonical name.

    Role nodes are shared across all politicians — "Député" is a single
    node regardless of how many deputies we load.
    """
    tx.run(
        _UPSERT_ROLE_QUERY,
        name=role.name,
        wikidata_id=role.wikidata_id,
        level=role.level,
    )


# ---------------------------------------------------------------------------
# Membership relationship upserts
# ---------------------------------------------------------------------------

_UPSERT_MEMBERSHIP_QUERY = """
MATCH (person:Person {wikidata_id: $person_id})
MATCH (party:Party  {wikidata_id: $party_id})

// We identify a specific membership interval by the combination of
// person, party, and start date.  This keeps multiple historical
// memberships for the same party distinct.
MERGE (person)-[r:MEMBER_OF {
  from_date: $from_date
}]->(party)
SET
  r.to_date    = $to_date,
  r.is_current = $is_current
RETURN type(r) AS rel_type
"""


def upsert_membership(
    tx: ManagedTransaction,
    person_id: str,
    party_id: str,
    from_date: Optional[str],
    to_date: Optional[str],
    is_current: bool,
) -> None:
    """
    MERGE a MEMBER_OF relationship between a Person and a Party.

    The relationship is uniquely identified by (person, party, from_date).
    A person who leaves and rejoins a party will have two separate MEMBER_OF
    relationships with different from_date values.

    Silently skips if either Person or Party node does not exist yet
    (the MATCH will simply return no rows rather than raising an error).
    """
    tx.run(
        _UPSERT_MEMBERSHIP_QUERY,
        person_id=person_id,
        party_id=party_id,
        from_date=from_date,
        to_date=to_date,
        is_current=is_current,
    )


# ---------------------------------------------------------------------------
# Mandate (HELD_ROLE) relationship upserts
# ---------------------------------------------------------------------------

_UPSERT_MANDATE_QUERY = """
MATCH (person:Person {wikidata_id: $person_id})
MATCH (role:Role     {name: $role_name})

// A mandate is identified by (person, role, from_date).
// A person can be a "Député" in multiple separate terms.
MERGE (person)-[r:HELD_ROLE {
  from_date: $from_date
}]->(role)
SET
  r.to_date      = $to_date,
  r.is_current   = $is_current,
  r.constituency = $constituency
RETURN type(r) AS rel_type
"""


def upsert_mandate(
    tx: ManagedTransaction,
    person_id: str,
    role_name: str,
    from_date: Optional[str],
    to_date: Optional[str],
    is_current: bool,
    constituency: Optional[str],
) -> None:
    """
    MERGE a HELD_ROLE relationship between a Person and a Role.

    Both the Person and Role nodes must already exist before calling this.
    The role node is matched by its canonical name.
    """
    tx.run(
        _UPSERT_MANDATE_QUERY,
        person_id=person_id,
        role_name=role_name,
        from_date=from_date,
        to_date=to_date,
        is_current=is_current,
        constituency=constituency,
    )


# ---------------------------------------------------------------------------
# Batch helpers
# ---------------------------------------------------------------------------

_BATCH_SIZE = 500  # commit every N records to bound transaction memory


def _load_persons_batch(persons: list[Person]) -> int:
    """Load all Person nodes in batched write transactions."""
    driver = get_driver()
    loaded = 0
    for i in range(0, len(persons), _BATCH_SIZE):
        batch = persons[i : i + _BATCH_SIZE]
        with driver.session() as session:
            for person in batch:
                session.execute_write(upsert_person, person)
            loaded += len(batch)
        logger.info("  Persons: %d / %d loaded", loaded, len(persons))
    return loaded


def _load_parties_batch(parties: list[Party]) -> int:
    """Load all Party nodes in batched write transactions."""
    driver = get_driver()
    loaded = 0
    for i in range(0, len(parties), _BATCH_SIZE):
        batch = parties[i : i + _BATCH_SIZE]
        with driver.session() as session:
            for party in batch:
                session.execute_write(upsert_party, party)
            loaded += len(batch)
        logger.info("  Parties: %d / %d loaded", loaded, len(parties))
    return loaded


def _load_roles_batch(role_names: set[str]) -> int:
    """
    Derive Role nodes from the set of unique role names found in mandates.

    Role level is inferred from the name using a simple keyword map.
    """
    level_map: dict[str, str] = {
        "Député européen": "european",
        "Président de la République": "national",
        "Premier ministre": "national",
        "Ministre": "national",
        "Secrétaire d'État": "national",
        "Préfet": "national",
        "Député": "national",
        "Sénateur": "national",
        "Président de région": "regional",
        "Conseiller régional": "regional",
        "Conseiller départemental": "regional",
        "Maire": "local",
    }

    driver = get_driver()
    loaded = 0
    roles = [
        Role(name=name, level=level_map.get(name, "national"))
        for name in sorted(role_names)
    ]

    with driver.session() as session:
        for role in roles:
            session.execute_write(upsert_role, role)
            loaded += 1

    logger.info("  Roles: %d unique roles loaded", loaded)
    return loaded


def _load_memberships_batch(memberships: list[PartyMembership]) -> int:
    """Load MEMBER_OF relationships."""
    driver = get_driver()
    loaded = 0
    skipped = 0

    for i in range(0, len(memberships), _BATCH_SIZE):
        batch = memberships[i : i + _BATCH_SIZE]
        with driver.session() as session:
            for m in batch:
                try:
                    session.execute_write(
                        upsert_membership,
                        m.person_wikidata_id,
                        m.party_wikidata_id,
                        m.from_date,
                        m.to_date,
                        m.is_current,
                    )
                    loaded += 1
                except Exception as exc:
                    logger.debug("Skipping membership %s->%s: %s", m.person_wikidata_id, m.party_wikidata_id, exc)
                    skipped += 1
        logger.info(
            "  Memberships: %d / %d loaded (%d skipped — missing nodes)",
            loaded,
            len(memberships),
            skipped,
        )
    return loaded


def _load_mandates_batch(mandates: list[Mandate]) -> int:
    """Load HELD_ROLE relationships."""
    driver = get_driver()
    loaded = 0
    skipped = 0

    for i in range(0, len(mandates), _BATCH_SIZE):
        batch = mandates[i : i + _BATCH_SIZE]
        with driver.session() as session:
            for m in batch:
                try:
                    session.execute_write(
                        upsert_mandate,
                        m.person_wikidata_id,
                        m.role_name,
                        m.from_date,
                        m.to_date,
                        m.is_current,
                        m.constituency,
                    )
                    loaded += 1
                except Exception as exc:
                    logger.debug("Skipping mandate %s->%s: %s", m.person_wikidata_id, m.role_name, exc)
                    skipped += 1
        logger.info(
            "  Mandates: %d / %d loaded (%d skipped — missing nodes)",
            loaded,
            len(mandates),
            skipped,
        )
    return loaded


# ---------------------------------------------------------------------------
# Full ETL orchestration
# ---------------------------------------------------------------------------

def run_full_load(
    page_size: Optional[int] = None,
    max_pages: int = 0,
) -> None:
    """
    Run the complete ETL pipeline:

    1. Apply schema constraints and indexes.
    2. Scrape politicians from Wikidata.
    3. Scrape party memberships + derive Party nodes.
    4. Scrape mandates + derive Role nodes.
    5. Load all nodes into Neo4j (Person, Party, Role).
    6. Load all relationships (MEMBER_OF, HELD_ROLE).

    Parameters
    ----------
    page_size:
        Number of results per SPARQL page.  Defaults to settings.etl_page_size.
    max_pages:
        0 = fetch all data (may take 10–30 minutes for the full Wikidata dataset).
        Set to 1 for a quick smoke test (~200 politicians).
    """
    logger.info("=" * 60)
    logger.info("French Politics Graph — Full ETL Load starting")
    logger.info("=" * 60)

    # Step 1: Ensure schema is in place
    logger.info("[1/6] Applying schema constraints and indexes...")
    run_constraints()

    # Step 2: Scrape politicians
    logger.info("[2/6] Fetching politicians from Wikidata...")
    persons = fetch_politicians(page_size=page_size, max_pages=max_pages)
    logger.info("      -> %d politicians fetched.", len(persons))

    # Step 3: Scrape party memberships + parties
    logger.info("[3/6] Fetching party memberships from Wikidata...")
    memberships, parties = fetch_party_memberships(page_size=page_size, max_pages=max_pages)
    logger.info("      -> %d memberships, %d parties fetched.", len(memberships), len(parties))

    # Step 4: Scrape mandates
    logger.info("[4/6] Fetching mandates from Wikidata...")
    mandates = fetch_mandates(page_size=page_size, max_pages=max_pages)
    logger.info("      -> %d mandates fetched.", len(mandates))

    # Step 5: Load nodes
    logger.info("[5/6] Loading nodes into Neo4j...")

    logger.info("  Loading Person nodes...")
    n_persons = _load_persons_batch(persons)

    logger.info("  Loading Party nodes...")
    n_parties = _load_parties_batch(parties)

    logger.info("  Loading Role nodes...")
    role_names = {m.role_name for m in mandates}
    n_roles = _load_roles_batch(role_names)

    # Step 6: Load relationships
    logger.info("[6/6] Loading relationships into Neo4j...")

    logger.info("  Loading MEMBER_OF relationships...")
    n_memberships = _load_memberships_batch(memberships)

    logger.info("  Loading HELD_ROLE relationships...")
    n_mandates = _load_mandates_batch(mandates)

    # Summary
    logger.info("=" * 60)
    logger.info("ETL Complete. Summary:")
    logger.info("  Person nodes:      %d", n_persons)
    logger.info("  Party nodes:       %d", n_parties)
    logger.info("  Role nodes:        %d", n_roles)
    logger.info("  MEMBER_OF rels:    %d", n_memberships)
    logger.info("  HELD_ROLE rels:    %d", n_mandates)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run the French Politics Graph ETL pipeline."
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=None,
        help="Number of results per SPARQL page (default: from config)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help=(
            "Max number of pages to fetch per query. "
            "0 = unlimited (full dataset). "
            "Use 1 for a quick smoke test."
        ),
    )
    args = parser.parse_args()

    try:
        run_full_load(page_size=args.page_size, max_pages=args.max_pages)
    finally:
        close_driver()
