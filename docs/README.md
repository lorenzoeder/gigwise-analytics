# GigWise Analytics — Documentation

## Project documentation

| Document | Contents |
|---|---|
| [User Guide](./user_guide.md) | Step-by-step setup and run instructions |
| [Design Choices](./design_choices.md) | Component-by-component rationale and known limitations |
| [Project Context](./project_context_and_objective.md) | Problem statement, architectural intent, success criteria |
| [Future Opportunities](./future_opportunities.md) | Ideas for extending the project |

## Component READMEs

Each pipeline component has its own README with implementation details:

| Component | Location |
|---|---|
| Ingestion (dlt) | [`dlt/README.md`](../dlt/README.md) |
| Orchestration (Bruin) | [`bruin/README.md`](../bruin/README.md) |
| Transformation (dbt) | [`dbt/README.md`](../dbt/README.md) |

## Architecture

```mermaid
flowchart LR
    TM[Ticketmaster API] --> DLT[dlt ingestion]
    SL[Setlist.fm API] --> DLT
    MB[MusicBrainz API] --> DLT

    subgraph Bruin[Bruin Orchestration]
        DLT --> RAW[(BigQuery raw)]
        RAW --> SQL[SQL Staging + Quality]
    end

    RAW --> DBT[dbt models]
    DBT --> ANA[(BigQuery analytics)]
    ANA --> DASH[Streamlit dashboard]

    KE[Kestra Scheduler] --> Bruin
    KE --> DBT
```

## dbt model lineage

```mermaid
flowchart TD
    R1[raw.ticketmaster_events] --> S1[stg_ticketmaster__events]
    R2[raw.setlistfm_setlists] --> S2[stg_setlistfm__setlists]
    R4[raw.musicbrainz_artists] --> S1

    S1 --> I1[int_concerts_unified]
    S2 --> I1

    R1 --> C1[dim_artist]
    R4 --> C1
    I1 --> C2[fact_concert]
    C1 --> C2

    C2 --> M1[mart_artist_touring_intensity]
    C1 --> M1

    C2 --> M2[mart_artist_yearly_repertoire]
    C1 --> M2

    C2 --> M3[mart_artist_setlist_freshness]
    C1 --> M3
```
