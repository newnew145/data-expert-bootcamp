INSERT INTO
    public.host_activity_reduced
WITH
    yesterday AS (
        SELECT
            *
        FROM
            host_activity_reduced
        WHERE
            date = '2023-01-02'
    ),
    today AS (
        SELECT
			EXTRACT(month from DATE(CAST(e.event_time AS TIMESTAMP))) as month,
            host,
            DATE(CAST(e.event_time AS TIMESTAMP)) AS host_activity_date,
            count(1) as hit_count,
			count(distinct user_id) as unique_visitor 
        FROM
            events e
        WHERE
            DATE(CAST(e.event_time AS TIMESTAMP)) = DATE('2023-01-03')
			AND
			user_id IS NOT NULL
        GROUP BY
            host,
            DATE(CAST(e.event_time AS TIMESTAMP))
    )
SELECT
	COALESCE(t.month, y.month) AS month,
    COALESCE(t.host, y.host) AS host,
	COALESCE(y.hit_array,
           array_fill(NULL::BIGINT, ARRAY[DATE('2023-01-03') - DATE('2023-01-01')]))
        || ARRAY[t.hit_count] AS hits_array,
	COALESCE(y.hit_array,
           array_fill(NULL::INTEGER, ARRAY[DATE('2023-01-03') - DATE('2023-01-01')]))
        || ARRAY[t.unique_visitor] AS unique_visitor,
	host_activity_date AS date
FROM
    today t
    FULL OUTER JOIN yesterday y ON t.host = y.host
    AND t.host = y.host
