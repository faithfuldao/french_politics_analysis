"""
Application configuration loaded from environment variables or a .env file.

Uses pydantic-settings so every setting is typed and validated at startup.
Create a .env file at the project root (alongside backend/) with at minimum:

    NEO4J_PASSWORD=your_password_here

All other settings have sensible defaults for local development.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"

    # Wikidata public SPARQL endpoint — no API key required
    wikidata_sparql_endpoint: str = "https://query.wikidata.org/sparql"

    # ETL behaviour
    # Number of politicians to fetch per SPARQL page (LIMIT per request)
    etl_page_size: int = 200
    # Maximum pages to fetch — set to 0 for no cap (full dataset)
    etl_max_pages: int = 0
    # Seconds to wait between SPARQL requests (polite crawling)
    etl_request_delay: float = 1.0
    # Maximum number of retries per failed SPARQL request
    etl_max_retries: int = 5


# Module-level singleton — import this everywhere instead of re-instantiating
settings = Settings()
