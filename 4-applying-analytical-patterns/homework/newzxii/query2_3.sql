WITH matches AS(
SELECT 
	COALESCE(gd.player_name, '(allplayer)') as player_name,
	COALESCE(gd.team_abbreviation,'(allteam)') as team_abbreviation,
	COUNT(CASE WHEN g.home_team_id = gd.team_id and g.home_team_wins = 1 THEN 1 ELSE 0 END) AS win_count 
FROM game_details gd
LEFT JOIN games g ON gd.game_id = g.game_id
GROUP BY GROUPING SETS (
					(gd.player_name ,gd.team_abbreviation),
					()
					)
)
select team_abbreviation
from matches
-- filter focus on team that win the most
where player_name <> '(allplayer)'
and team_abbreviation <> '(allteam)'
order by win_count desc
limit 1
;