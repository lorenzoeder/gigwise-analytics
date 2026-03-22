SELECT *
FROM {{ ref('fact_concert') }}
WHERE event_date > DATE_ADD(CURRENT_DATE(), INTERVAL 3 YEAR)
