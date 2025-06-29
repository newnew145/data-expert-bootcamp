INSERT INTO
	actors
WITH
    last_year AS (
        SELECT
            *
        FROM
            actors
        WHERE
            -- get min year from actor_films
            year = (SELECT MIN(year) FROM actor_films)
    ),
    current_year AS (
        SELECT
            actor,
            actorid,
            array_agg (ROW (film, votes, rating, filmid)::films_arr) AS films,
            -- Determining the 'quality_class' based on the average film rating
            CASE
                WHEN AVG(rating) > 8 THEN 'star'
                WHEN AVG(rating) > 7 THEN 'good'
                WHEN AVG(rating) > 6 THEN 'average'
                ELSE 'bad'
            END AS quality_class,
            year
        FROM
           actor_films
        WHERE
            -- year = last_year + 1 (current_year)
            year = (SELECT MIN(year) FROM actor_films) + 1
            -- Grouping the results by actor, actor_id, and year for aggregation
        GROUP BY
            actor,
            actorid,
            year
    )
SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.actor_id, cy.actorid) AS actor_id,
    CASE
        WHEN ly.films IS NULL THEN cy.films
        WHEN cy.films IS NOT NULL THEN ly.films || cy.films
        ELSE ly.films
    END AS films,
    COALESCE(cy.quality_class, ly.quality_class) AS quality_class,
    COALESCE(cy.actorid, ly.actor_id) IS NOT NULL AS is_active,
    COALESCE(cy.year, ly.year + 1) AS year
FROM
    last_year ly
    FULL OUTER JOIN current_year cy ON cy.actorid = ly.actor_id
    AND ly.year = cy.year - 1
