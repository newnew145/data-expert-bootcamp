INSERT INTO
	public.user_devices_cumulated
WITH
    yesterday AS (
        -- select all columns from 'user_devices_cumulated' table for the date '2023-01-01'
        SELECT
            *
        FROM
            user_devices_cumulated
        WHERE
            date = DATE ('2022-12-31')
    ),
    today AS (
		SELECT 
			CAST(e.user_id AS TEXT) AS user_id,
			d.browser_type,
			DATE(CAST(e.event_time AS TIMESTAMP)) as date_active
        FROM
            devices d
            LEFT JOIN events e ON d.device_id = e.device_id
        WHERE
			DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
			AND user_id IS NOT NULL
        GROUP BY
			user_id,
			d.browser_type,
			DATE(CAST(event_time AS TIMESTAMP))
    )
SELECT 
	p.user_id,
	jsonb_object_agg(p.browser_type, p.dates_active) as device_activity_datelist,
	p.dates_active,
	p.date
FROM (SELECT
	COALESCE(t.user_id, y.user_id) as user_id,
	COALESCE(t.browser_type) as browser_type,
	CASE
		WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.dates_active
		ELSE y.dates_active || ARRAY[t.date_active]
		END AS dates_active,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id) p
GROUP BY user_id, p.dates_active, p.date
