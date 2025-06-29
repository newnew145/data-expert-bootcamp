WITH date_list_act AS (
SELECT user_id,
		browser_type AS browser_type,
		CAST
			((
				CASE
					WHEN date_arr @> to_jsonb(generate_series)
					THEN CAST(POW (2, 31 - (date - generate_series)) AS BIGINT)
					ELSE 0
				END
			) AS BIT(32)
		) AS dates_active
FROM public.user_devices_cumulated,
jsonb_each(device_activity_datelist) AS t(browser_type, date_arr)
CROSS JOIN (
    SELECT generate_series(DATE('2023-01-01'), DATE('2023-01-02'), interval '1 day')::date) s
GROUP BY user_id, browser_type, date_arr, generate_series, date)
SELECT user_id,
	jsonb_object_agg(browser_type, dates_active) as device_activity_dateint
FROM date_list_act
GROUP BY user_id, dates_active
-- data only contain 1 day so filter by bit count = 1
HAVING bit_count(dates_active) = 1