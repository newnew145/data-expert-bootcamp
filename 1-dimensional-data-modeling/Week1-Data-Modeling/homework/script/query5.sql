CREATE TYPE ac_scd_type AS (
	quality_class VARCHAR,
	is_active boolean,
	start_date integer,
	end_date integer
);


INSERT INTO
    actors_history_scd
WITH
    last_year_scd AS (
        SELECT
            *
        FROM
            actors_history_scd
        WHERE
            year = 2020
    ),
    current_year_scd AS (
        SELECT
            *
        FROM
            public.actors
        WHERE
            year = 2021
    ),
	-- in case we have a data
	unchange_record AS (
		SELECT 
			cy.actor,
			cy.quality_class,
			cy.is_active,
			ly.start_date,
			cy.year AS end_date
		FROM 
			current_year_scd cy
		JOIN
			last_year_scd ly
		ON 
			ly.actor = cy.actor
		WHERE
			cy.quality_class = ly.quality_class AND cy.is_active = ly.is_active
	),
	change_record AS (
		SELECT
			cy.actor,
			UNNEST(ARRAY[
				ROW(
					ly.quality_class,
					ly.is_active,
					ly.start_date,
					ly.end_date
					)::ac_scd_type,
				ROW(
					cy.quality_class,
					cy.is_active,
					cy.year,
					cy.year
					)::ac_scd_type
			]) as records
		FROM current_year_scd cy
		LEFT JOIN last_year_scd ly
			ON ly.actor = cy.actor
		WHERE cy.quality_class <> ly.quality_class  
			OR cy.is_active <> ly.is_active
	)
	,
	unnest_records AS (
		SELECT 
			actor,
			(records::ac_scd_type).quality_class,
			(records::ac_scd_type).is_active,
			(records::ac_scd_type).start_date,
			(records::ac_scd_type).end_date
		FROM change_record
	),
	new_records AS (
		SELECT
			cy.actor,
			cy.quality_class,
			cy.is_active,
			cy.year AS start_date,
			cy.year AS end_date
		FROM current_year_scd cy
		LEFT JOIN last_year_scd ly
			ON cy.actor = ly.actor
		WHERE ly.actor IS NULL
	)
	SELECT *, 2021 AS year FROM (

                  SELECT *
                  FROM unchange_record

                  UNION ALL

                  SELECT *
                  FROM unnest_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a
	