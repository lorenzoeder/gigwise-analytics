/* @bruin
name: quality.check_event_dates
type: bq.sql
connection: bigquery
depends:
  - staging.stg_concerts_union
@bruin */

SELECT COUNT(*) AS invalid_rows
FROM raw.ticketmaster_events
WHERE DATE(event_date) > CURRENT_DATE()
