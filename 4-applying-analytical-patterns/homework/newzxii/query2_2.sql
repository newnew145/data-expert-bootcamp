WITH matches AS(
SELECT 
	COALESCE(gd.player_name, '(allplayer)') as player_name,
	COALESCE(g.season, 9999) as season,
	COALESCE(sum(gd.pts), 9999999) as scored
FROM game_details gd
LEFT JOIN games g ON gd.game_id = g.game_id
GROUP BY GROUPING SETS (
					(gd.player_name, g.season),
					()
					)
-- filtering overall from group set that may cause null value
having sum(gd.pts) <> 9999999 
order by scored desc
)
select player_name
from matches
-- filter only focus in one season for one player
where season <> 9999
order by scored desc
limit 1;