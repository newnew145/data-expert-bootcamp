INSERT INTO
    public.hosts_cumulated
WITH
    yesterday AS (
        SELECT
            *
        FROM
            public.hosts_cumulated
        WHERE
            date = DATE('2023-01-24')
    ),
    today AS (
        SELECT
            host,
            DATE(CAST(e.event_time AS TIMESTAMP)) AS host_activity_datelist,
            count(1)
        FROM
            events e
        WHERE
            DATE(CAST(e.event_time AS TIMESTAMP)) = DATE('2023-01-25')
        GROUP BY
            host,
            DATE(CAST(e.event_time AS TIMESTAMP))
    )
SELECT
    COALESCE(y.host, t.host) AS host,
    CASE
        WHEN y.host_activity_datelist IS NOT NULL THEN ARRAY[t.host_activity_datelist] || y.host_activity_datelist
        ELSE ARRAY[t.host_activity_datelist]
    END AS host_activity_datelist,
    DATE('2023-01-25') AS date
FROM
    yesterday y
    FULL OUTER JOIN today t ON y.host = t.host