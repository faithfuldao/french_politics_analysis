"""
News enricher — Phase 2 stub.

TODO (Phase 2): This module will scrape French news sources and legal databases
to enrich the knowledge graph with crime allegations and court convictions.

Planned pipeline:
  1. Fetch recent news articles mentioning politicians already in the graph
     (using names and aliases as search terms against media APIs such as
     NewsAPI, MediaStack, or direct RSS feeds of Le Monde / Libération /
     Le Figaro / Mediapart).

  2. Pass article text through an LLM (Claude or GPT-4) with a structured
     extraction prompt to identify:
       - Is a politician named in connection with a legal accusation?
       - What is the alleged crime category? (corruption, fraud, violence, …)
       - What is the date of the alleged act / the accusation?
       - Is there a confirmed court conviction? If so, what sentence?

  3. Validate the LLM output against Pydantic Crime / Conviction models.

  4. Upsert into Neo4j:
       MERGE (c:Crime {crime_id: $crime_id})
       SET c += $props
       WITH c
       MATCH (p:Person {wikidata_id: $person_id})
       MERGE (p)-[:ACCUSED_OF {date: $date, source_url: $url}]->(c)

       MERGE (cv:Conviction {conviction_id: $conviction_id})
       SET cv += $props
       WITH cv
       MATCH (p:Person {wikidata_id: $person_id})
       MERGE (p)-[:CONVICTED_OF {date: $date, sentence: $sentence, source_url: $url}]->(cv)

  5. Store source URLs on the relationship (ACCUSED_OF / CONVICTED_OF) so the
     frontend can surface provenance links for every claim.

  6. Implement a deduplication pass: if the same article has already been
     processed (tracked via a hash of the URL), skip it to avoid phantom nodes.

  7. Respect robots.txt for all scraped domains and rate-limit requests.
"""
