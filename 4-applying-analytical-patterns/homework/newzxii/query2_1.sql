
WITH matches AS(
SELECT 
	COALESCE(gd.player_name, '(allplayer)') as player_name,
	COALESCE(gd.team_abbreviation,'(allteam)') as team_abbreviation,
	COALESCE(sum(gd.pts), 9999999) as scored
FROM game_details gd
LEFT JOIN games g ON gd.game_id = g.game_id
GROUP BY GROUPING SETS (
					(gd.player_name, gd.team_abbreviation),
					()
					)
-- filtering overall from group set that may cause null value
having sum(gd.pts) <> 9999999 
order by scored desc
)
select player_name
from matches
-- filter groupset data focus on 1 team at all season(9999)
where player_name <> '(allplayer)' 
and team_abbreviation <> '(allteam)'
order by scored desc
limit 1;