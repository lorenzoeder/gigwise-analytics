# GigWise Analytics — Future Opportunities

Ideas for extending the project beyond its current scope.

---

## Data Source Expansion

- **Last.fm API** — Playcount and listener statistics as an artist popularity proxy, enabling "How does touring relate to popularity?" analysis.
- **Additional Ticketmaster markets** — Expand beyond US, CA, GB, DE, IT to cover more of Europe, Asia, and Latin America.
- **Bandsintown API** — Independent event data source to cross-validate Ticketmaster coverage and add smaller venues.

---

## Analytics Enhancements

- **Tour detection** — Use Setlist.fm `tour_name` and clustering on date/geography to identify distinct tours and measure tour-level metrics (length, geographic spread, setlist consistency).
- **Song popularity trends** — Track which songs appear most frequently across setlists over time, identifying "always played" anthems vs. rotating deep cuts.
- **Venue analytics** — Venue capacity and type analysis (arena vs. club vs. festival) to understand artist trajectory.
- **Geographic heatmaps** — Map-based visualization of concert density by city/region.
- **Ticket price analysis** — If Ticketmaster expands price data availability beyond the current GB market limitation, add pricing trends by artist, genre, and market.

---

## Infrastructure & Pipeline Improvements

- **Separate canonical artist ID** — Replace the current MBID-based `artist_id` with a dedicated internal identifier to decouple the data model from MusicBrainz.
- **Monitoring and observability** — Add metric tables tracking genre coverage rate, MBID resolution rate, and ingestion row counts per run. Surface these in a dedicated dashboard panel or alerting system.
- **CI/CD pipeline** — Automated dbt tests and Terraform plan on pull requests. GitHub Actions or similar.
- **Data contracts** — Formalize the expected schema from each API source to catch upstream API changes early.
- **dbt model versioning** — Add model versioning to manage breaking changes to mart schemas consumed by the dashboard and downstream tools.

---

## Dashboard & User Experience

- **Artist search and filtering** — Add free-text search and multi-select filters across all tiles, rather than a single dropdown.
- **Time range selector** — Allow users to filter the touring intensity view to a custom date window.
- **Comparison mode** — Side-by-side artist comparisons for setlist evolution and touring intensity.
- **Export and sharing** — CSV/PDF export of dashboard views for reports.
- **Mobile layout** — Responsive Streamlit layout for smaller screens.

---

## Streaming Evolution

- **Integrate streaming into dbt** — Currently, Kafka consumer writes to a separate `streaming` dataset. A future version could merge streaming data into the main analytics models for a unified view.
- **Event change detection** — Track how events change over time (date moved, venue changed, cancelled) to surface volatility metrics.
- **Multi-source streaming** — Add Setlist.fm or social media event feeds as additional streaming sources.
- **Alerting** — Notify users when a tracked artist announces new dates or a concert sell-out pattern is detected.

---

## Cost & Scale

- **BigQuery slot reservations** — At scale, move from on-demand to flat-rate pricing if query volume justifies it.
- **Spark on larger clusters** — Current Dataproc Serverless runs with minimal resources. For a full music industry dataset, scale up cluster size and explore Delta Lake or Iceberg for the data lake layer.
- **Caching layer** — Add Redis or similar caching in front of BigQuery for frequently accessed dashboard queries.
