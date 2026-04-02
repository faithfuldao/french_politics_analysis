// =============================================================================
// Neo4j Schema: Constraints and Indexes
// French Politics Knowledge Graph — Phase 1
//
// Run this file once against a fresh Neo4j instance before any ETL load.
// Execution order matters: constraints create implicit indexes so we define
// them first, then add any extra composite or text indexes below.
//
// Compatible with Neo4j 5.x (syntax differs from 4.x — no "ON" keyword).
// =============================================================================


// ---------------------------------------------------------------------------
// PERSON
// ---------------------------------------------------------------------------

// Uniqueness constraint — also creates a backing b-tree index on wikidata_id
CREATE CONSTRAINT person_wikidata_id_unique IF NOT EXISTS
FOR (p:Person)
REQUIRE p.wikidata_id IS UNIQUE;

// Additional index for name-based lookups (autocomplete, search)
CREATE INDEX person_name_index IF NOT EXISTS
FOR (p:Person)
ON (p.name);

// Text index for full-text search on person names
CREATE TEXT INDEX person_name_text IF NOT EXISTS
FOR (p:Person)
ON (p.name);


// ---------------------------------------------------------------------------
// PARTY
// ---------------------------------------------------------------------------

CREATE CONSTRAINT party_wikidata_id_unique IF NOT EXISTS
FOR (p:Party)
REQUIRE p.wikidata_id IS UNIQUE;

CREATE INDEX party_name_index IF NOT EXISTS
FOR (p:Party)
ON (p.name);

CREATE TEXT INDEX party_name_text IF NOT EXISTS
FOR (p:Party)
ON (p.name);


// ---------------------------------------------------------------------------
// ROLE
// ---------------------------------------------------------------------------

// Roles are identified by their canonical French name (e.g. "Député")
CREATE CONSTRAINT role_name_unique IF NOT EXISTS
FOR (r:Role)
REQUIRE r.name IS UNIQUE;

// Index to support lookups by role level (national / regional / local)
CREATE INDEX role_level_index IF NOT EXISTS
FOR (r:Role)
ON (r.level);


// ---------------------------------------------------------------------------
// CRIME
// ---------------------------------------------------------------------------

CREATE CONSTRAINT crime_id_unique IF NOT EXISTS
FOR (c:Crime)
REQUIRE c.crime_id IS UNIQUE;

CREATE INDEX crime_category_index IF NOT EXISTS
FOR (c:Crime)
ON (c.category);


// ---------------------------------------------------------------------------
// CONVICTION
// ---------------------------------------------------------------------------

CREATE CONSTRAINT conviction_id_unique IF NOT EXISTS
FOR (c:Conviction)
REQUIRE c.conviction_id IS UNIQUE;

// Index to support filtering by final/non-final convictions
CREATE INDEX conviction_is_final_index IF NOT EXISTS
FOR (c:Conviction)
ON (c.is_final);


// ---------------------------------------------------------------------------
// ORGANIZATION
// ---------------------------------------------------------------------------

CREATE CONSTRAINT organization_wikidata_id_unique IF NOT EXISTS
FOR (o:Organization)
REQUIRE o.wikidata_id IS UNIQUE;

CREATE INDEX organization_name_index IF NOT EXISTS
FOR (o:Organization)
ON (o.name);

CREATE INDEX organization_type_index IF NOT EXISTS
FOR (o:Organization)
ON (o.org_type);


// ---------------------------------------------------------------------------
// Relationship property indexes
// These speed up filtering on relationship properties without materialising
// intermediate nodes — supported in Neo4j 5.x.
// ---------------------------------------------------------------------------

// Filter current party memberships quickly
CREATE INDEX member_of_is_current IF NOT EXISTS
FOR ()-[r:MEMBER_OF]-()
ON (r.is_current);

// Filter current mandates quickly
CREATE INDEX held_role_is_current IF NOT EXISTS
FOR ()-[r:HELD_ROLE]-()
ON (r.is_current);

// Date-range queries on mandates
CREATE INDEX held_role_from_date IF NOT EXISTS
FOR ()-[r:HELD_ROLE]-()
ON (r.from_date);
