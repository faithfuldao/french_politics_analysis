"""
Pydantic models for the French Politics Knowledge Graph.

These models represent the core entities scraped from Wikidata and loaded
into Neo4j. All date fields are stored as ISO-8601 strings (YYYY-MM-DD)
and are nullable to handle incomplete Wikidata records.
"""

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field


class Person(BaseModel):
    """
    A French politician scraped from Wikidata.

    Node label: Person
    Unique constraint: wikidata_id
    Index: name
    """

    wikidata_id: str = Field(..., description="Wikidata Q-identifier, e.g. Q1234")
    name: str = Field(..., description="Full name in French (fr label)")
    date_of_birth: Optional[str] = Field(None, description="ISO date YYYY-MM-DD")
    date_of_death: Optional[str] = Field(None, description="ISO date YYYY-MM-DD, null if alive")
    gender: Optional[str] = Field(None, description="male | female | other")
    wikipedia_fr_url: Optional[str] = Field(None, description="French Wikipedia article URL")
    image_url: Optional[str] = Field(None, description="Wikimedia Commons image URL")


class Party(BaseModel):
    """
    A French political party or political group.

    Node label: Party
    Unique constraint: wikidata_id
    Index: name
    """

    wikidata_id: str = Field(..., description="Wikidata Q-identifier")
    name: str = Field(..., description="Party name in French")
    short_name: Optional[str] = Field(None, description="Acronym or short name, e.g. LFI")
    founded_date: Optional[str] = Field(None, description="ISO date YYYY-MM-DD")
    dissolved_date: Optional[str] = Field(None, description="ISO date YYYY-MM-DD, null if active")
    ideology: Optional[str] = Field(None, description="Political ideology label from Wikidata")


class Role(BaseModel):
    """
    A political role or office that a person can hold.
    Roles are generic (e.g. "Deputé", "Sénateur", "Ministre") and shared
    across all persons — they are not tied to a specific mandate instance.

    Node label: Role
    Unique constraint: name
    """

    name: str = Field(..., description="Role name in French, e.g. 'Député', 'Sénateur'")
    wikidata_id: Optional[str] = Field(None, description="Wikidata Q-identifier for this role type")
    level: Optional[str] = Field(
        None,
        description="Government level: national | regional | local | european",
    )


class Mandate(BaseModel):
    """
    A specific instance of a person holding a Role (their mandate).

    This is modelled as a relationship property bag — Mandate is NOT a
    node in Neo4j. It provides the data for the HELD_ROLE relationship:
      (Person)-[:HELD_ROLE {from_date, to_date, is_current, constituency}]->(Role)
    """

    person_wikidata_id: str
    role_name: str
    from_date: Optional[str] = Field(None, description="ISO date YYYY-MM-DD")
    to_date: Optional[str] = Field(None, description="ISO date YYYY-MM-DD")
    is_current: bool = False
    constituency: Optional[str] = Field(None, description="Circonscription or department name")
    wikidata_position_id: Optional[str] = Field(None, description="Wikidata Q-id of the position held")


class PartyMembership(BaseModel):
    """
    A person's membership in a political party during a time window.

    Modelled as relationship properties on MEMBER_OF:
      (Person)-[:MEMBER_OF {from_date, to_date, is_current}]->(Party)

    Multiple memberships (past and present) are stored as separate
    MEMBER_OF relationships differentiated by from_date.
    """

    person_wikidata_id: str
    party_wikidata_id: str
    from_date: Optional[str] = Field(None, description="ISO date YYYY-MM-DD")
    to_date: Optional[str] = Field(None, description="ISO date YYYY-MM-DD")
    is_current: bool = False


class Crime(BaseModel):
    """
    An accusation or alleged crime linked to a person.
    Populated in Phase 2 by the news enricher.

    Node label: Crime
    Unique constraint: crime_id
    """

    crime_id: str = Field(..., description="Unique identifier (slug or UUID)")
    label: str = Field(..., description="Short description of the alleged crime")
    category: Optional[str] = Field(
        None,
        description="Category: corruption | fraud | violence | sexual | other",
    )
    date: Optional[str] = Field(None, description="ISO date YYYY-MM-DD of alleged act")
    source_url: Optional[str] = Field(None, description="News article or court document URL")
    description: Optional[str] = Field(None, description="Longer free-text description")


class Conviction(BaseModel):
    """
    A court conviction of a person.
    Populated in Phase 2 by the news enricher.

    Node label: Conviction
    Unique constraint: conviction_id
    """

    conviction_id: str = Field(..., description="Unique identifier (slug or UUID)")
    label: str = Field(..., description="Short description of the conviction")
    date: Optional[str] = Field(None, description="ISO date YYYY-MM-DD of conviction")
    sentence: Optional[str] = Field(None, description="Sentence handed down, free text")
    court: Optional[str] = Field(None, description="Name of the court")
    source_url: Optional[str] = Field(None, description="Source URL for the conviction record")
    is_final: bool = Field(False, description="True if no further appeal is possible")


class Organization(BaseModel):
    """
    A non-party organization a politician may be affiliated with:
    think tanks, lobbies, NGOs, companies, etc.

    Node label: Organization
    Unique constraint: wikidata_id
    """

    wikidata_id: str = Field(..., description="Wikidata Q-identifier")
    name: str = Field(..., description="Organization name")
    org_type: Optional[str] = Field(
        None,
        description="Type: think_tank | lobby | ngo | company | media | other",
    )
    country: Optional[str] = Field(None, description="Country of registration")
