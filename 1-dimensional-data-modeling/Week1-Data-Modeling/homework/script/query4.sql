INSERT INTO
	actors_history_scd
WITH lagged AS (
SELECT
	actor,
	-- convert boolean from is_active to 1 or 0 for sum when streak
	CASE
		WHEN is_active THEN 1
		ELSE 0
	END AS is_active,
	-- using lag for coparison between current and last year (previous)
	CASE
		WHEN LAG(is_active, 1) OVER (
			PARTITION BY
				actor
			ORDER BY
				year
		) THEN 1
		ELSE 0
	END AS is_active_last_year,
	year,
	quality_class,
	LAG(quality_class, 1) OVER (
		PARTITION BY
			actor
		ORDER BY
			year
	) as last_year_quality_class
FROM
	actors
),
streaked AS (
SELECT
	*,
	-- Generating an identifier for each streak based on changes in 'is_active' status
	SUM(
		CASE
			WHEN is_active <> is_active_last_year THEN 1
			ELSE 0
		END
	) OVER (
		PARTITION BY
			actor
		ORDER BY
			year
	) AS identifier,
	SUM(
		CASE
			WHEN quality_class <> last_year_quality_class THEN 1
			ELSE 0
		END
	) OVER (
		PARTITION BY
			actor
		ORDER BY
			year
	) AS quality_class_identifier
	
FROM
	lagged
)
SELECT
    actor,
	quality_class,
    MAX(is_active) = 1 AS is_active,
    MIN(year) AS start_date,
    MAX(year) AS end_date,
    -- Setting the year for all records to 2020 (second to lastest year from actor films)
    2020 AS year
FROM
    streaked
GROUP BY
    actor,
	quality_class,
    identifier,
    quality_class_identifier
