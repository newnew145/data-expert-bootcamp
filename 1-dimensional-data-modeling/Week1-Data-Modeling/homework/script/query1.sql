CREATE TYPE films_arr AS (
		film TEXT,
		votes INTEGER,
		rating REAL,
		filmid TEXT
	);
CREATE TABLE public.actors (
	-- adding actor and actor_id (pk) for films data with associate with data
	actor VARCHAR NOT NULL,
	actor_id VARCHAR NOT NULL,
	-- Array for multiple films
	films films_arr[],
	quality_class VARCHAR,
	is_active BOOLEAN,
	-- movie year relavant with actor
	year INTEGER,
	PRIMARY KEY(actor_id, year)
)