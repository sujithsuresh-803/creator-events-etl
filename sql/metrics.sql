-- Analytical queries over the gold table (daily_artist_metrics).
-- Written in Spark SQL, but standard enough to run on most warehouses
-- (BigQuery/Snowflake/Redshift) with minor dialect tweaks.
-- Demonstrates: CTEs, window functions, ranking, and rolling aggregates.

-- 1) Top 10 breakout artists this month by 7-day rolling streams.
WITH ranked AS (
    SELECT
        artist_id,
        archetype,
        event_date,
        streams_7d,
        ROW_NUMBER() OVER (
            PARTITION BY artist_id ORDER BY event_date DESC
        ) AS recency
    FROM daily_artist_metrics
)
SELECT artist_id, archetype, streams_7d
FROM ranked
WHERE recency = 1
ORDER BY streams_7d DESC
LIMIT 10;


-- 2) Day-over-day streams growth per artist (LAG window function).
SELECT
    artist_id,
    event_date,
    streams,
    streams - LAG(streams) OVER (
        PARTITION BY artist_id ORDER BY event_date
    ) AS dod_change
FROM daily_artist_metrics
ORDER BY artist_id, event_date;


-- 3) Engagement mix by archetype — which creator type drives the most action.
SELECT
    archetype,
    SUM(streams)            AS streams,
    SUM(saves)              AS saves,
    SUM(follows)            AS follows,
    SUM(engagement_score)   AS engagement,
    ROUND(SUM(engagement_score) * 1.0 / SUM(streams), 2) AS engagement_per_stream
FROM daily_artist_metrics
GROUP BY archetype
ORDER BY engagement DESC;


-- 4) Each artist's best day, and how far the latest day is below their peak.
WITH peak AS (
    SELECT artist_id, MAX(streams) AS peak_streams
    FROM daily_artist_metrics
    GROUP BY artist_id
),
latest AS (
    SELECT artist_id, streams AS latest_streams
    FROM (
        SELECT artist_id, streams,
               ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY event_date DESC) AS rn
        FROM daily_artist_metrics
    ) t
    WHERE rn = 1
)
SELECT
    p.artist_id,
    p.peak_streams,
    l.latest_streams,
    p.peak_streams - l.latest_streams AS below_peak
FROM peak p
JOIN latest l USING (artist_id)
ORDER BY below_peak DESC;
