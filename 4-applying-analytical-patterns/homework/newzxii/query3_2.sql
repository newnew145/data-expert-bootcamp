--query for games in a row did LeBron James score over 10 points a game?

WITH Lebron_tbl AS (
SELECT 
	g.game_date_est as match_date,
	gd.player_name,
	pts,
	ROW_NUMBER() OVER (ORDER BY g.game_date_est) AS rn
FROM game_details gd
LEFT JOIN games g ON gd.game_id = g.game_id
where gd.player_name LIKE 'LeBron%'
order by match_date asc
)
SELECT max(match_date - previous_bad_game) as game_in_a_row
FROM (select match_date, pts, rn,
-- using lag to compare a match date
LAG(match_date) OVER (ORDER BY match_date) AS previous_bad_game
from Lebron_tbl
where pts < 10 
-- game that lebron may not play null value open this mark if it count and it will use null
-- if use null lebron will make more than 10pts = 365 games
-- or pts IS NULL
) f