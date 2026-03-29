# Project Context and Objective

## Problem we are solving

The project aims to answer a practical analytics question:

How can we combine live concert listings, historical setlists, and artist metadata into one coherent analytical dataset that supports human decisions and storytelling?

Typical user questions:

- Which artists are touring heavily across countries?
- What genres dominate the live touring landscape?
- How does an artist's setlist repertoire evolve over years of touring?

## Why this is hard

The sources were not designed to be joined cleanly:

- Ticketmaster is event-forward and future-oriented.
- Setlist.fm is performance-history-forward and backward-looking.
- MusicBrainz is a canonical identity resolver, but matching still depends on query quality and API reliability.

A key reality follows from this:

A strict event-date join between Ticketmaster and Setlist.fm will often be sparse, especially for future concerts, because future concerts usually do not yet have a published setlist.

This is expected behavior, not necessarily a pipeline failure.

## Project objective (current phase)

The objective is not just to run jobs without errors. The objective is to produce analytically meaningful data with explicit assumptions and known limits.

That means:

- ingestion should be robust and idempotent
- artist identity linkage should be recoverable for a meaningful subset of events
- model logic should prefer data correctness over superficial completeness
- limitations should be documented and visible, not hidden

## Architectural intent

Current intent by layer:

- dlt owns API ingestion and writes raw tables.
- dbt owns semantic transformation into staging, core, and marts.
- Bruin orchestrates and runs quality checks around ingestion outputs.
- Streamlit consumes marts for user-facing analytics.

This separation keeps responsibilities clear and makes failures easier to diagnose.

## Canonical key strategy

The project treats MusicBrainz ID (MBID) as the cross-source artist key wherever possible.

Why MBID:

- names are inconsistent (spacing, punctuation, aliases, casing)
- platform-specific IDs are not portable across data sources
- Ticketmaster and Setlist.fm artist naming can differ

When MBID is missing, the system falls back to name normalization and resolver lookups, but those are probabilistic and should be treated as enrichment, not guaranteed truth.

## Success criteria

A run is considered successful only when all are true:

- ingestion completes and upserts without duplicate explosions
- dbt build and tests pass
- fact_concert has non-trivial artist_id coverage
- marts contain enough rows to power dashboard slices
- docs explain what data means and what it does not mean

## What is intentionally out of scope in this phase

- perfect artist identity resolution for every event
- guaranteed Ticketmaster-to-Setlist record-level joins
- ticket price analysis (Ticketmaster GB market does not expose prices)
- production SLA guarantees

These are valid future improvements, but not required for the prototype objective.