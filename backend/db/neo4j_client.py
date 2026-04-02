"""
Neo4j connection pool and helper utilities.

Usage
-----
    from backend.db.neo4j_client import get_driver, run_constraints

The module exposes a lazily-initialised driver singleton.  Call `close_driver()`
at application shutdown (FastAPI lifespan event, or at the end of a CLI script).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Optional

from neo4j import GraphDatabase, Driver, ManagedTransaction

from backend.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Driver singleton
# ---------------------------------------------------------------------------

_driver: Optional[Driver] = None


def get_driver() -> Driver:
    """
    Return the module-level Neo4j driver, creating it on first call.

    The driver manages a connection pool internally.  A single Driver instance
    is safe to share across threads and async tasks.
    """
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
            # Keep connections warm for ETL workloads
            max_connection_lifetime=3600,
            max_connection_pool_size=50,
            connection_acquisition_timeout=30,
        )
        _driver.verify_connectivity()
        logger.info("Neo4j driver connected to %s", settings.neo4j_uri)
    return _driver


def close_driver() -> None:
    """Close the driver and release all pooled connections."""
    global _driver
    if _driver is not None:
        _driver.close()
        _driver = None
        logger.info("Neo4j driver closed")


# ---------------------------------------------------------------------------
# Transaction helpers
# ---------------------------------------------------------------------------

def run_in_transaction(
    work: Callable[[ManagedTransaction], Any],
    *,
    database: Optional[str] = None,
    write: bool = True,
) -> Any:
    """
    Execute `work(tx)` inside a managed transaction.

    Parameters
    ----------
    work:
        A callable that receives a ManagedTransaction and returns a value.
    database:
        Optional Neo4j database name (defaults to the server default).
    write:
        True for write transactions, False for read-only.
    """
    driver = get_driver()
    with driver.session(database=database) as session:
        if write:
            return session.execute_write(work)
        else:
            return session.execute_read(work)


def run_cypher(
    query: str,
    params: Optional[dict[str, Any]] = None,
    *,
    write: bool = True,
    database: Optional[str] = None,
) -> list[dict[str, Any]]:
    """
    Execute a single parameterised Cypher statement and return all rows.

    This is a convenience wrapper for one-off queries.  For ETL bulk loads
    use `run_in_transaction` with a dedicated function to amortise round-trips.

    Parameters
    ----------
    query:
        A Cypher string with $param placeholders.  NEVER build this via
        string concatenation — always use the `params` dict.
    params:
        Dictionary of parameter values to bind.
    write:
        True for write operations, False for read-only.
    database:
        Optional Neo4j database name.
    """
    params = params or {}

    def _work(tx: ManagedTransaction) -> list[dict[str, Any]]:
        result = tx.run(query, **params)
        return [dict(record) for record in result]

    return run_in_transaction(_work, write=write, database=database)


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------

def run_constraints(constraints_path: Optional[Path] = None) -> None:
    """
    Execute every statement in constraints.cypher against the connected database.

    Statements are split on semicolons so the file can contain multiple
    CREATE CONSTRAINT / CREATE INDEX directives.

    Parameters
    ----------
    constraints_path:
        Override the default path (useful in tests).  Defaults to the
        constraints.cypher file sitting next to this module.
    """
    if constraints_path is None:
        constraints_path = Path(__file__).parent / "constraints.cypher"

    cypher_text = constraints_path.read_text(encoding="utf-8")

    # Strip comment lines and split on semicolons
    statements = [
        stmt.strip()
        for stmt in cypher_text.split(";")
        if stmt.strip() and not stmt.strip().startswith("//")
    ]

    driver = get_driver()
    with driver.session() as session:
        for stmt in statements:
            # Skip pure-comment blocks that survived the split
            lines = [l for l in stmt.splitlines() if not l.strip().startswith("//")]
            clean = "\n".join(lines).strip()
            if not clean:
                continue
            try:
                session.run(clean)
                logger.debug("Executed: %s", clean[:80])
            except Exception as exc:
                logger.warning("Constraint/index statement failed (may already exist): %s", exc)

    logger.info("Schema constraints and indexes applied from %s", constraints_path)
