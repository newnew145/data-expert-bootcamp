WITH matches AS(
SELECT 
	COALESCE(gd.player_name, '(allplayer)') as player_name,
	COALESCE(gd.team_abbreviation,'(allteam)') as team_abbreviation,
	COALESCE(g.season, 9999) as season,
	COALESCE(sum(gd.pts), 9999999) as scored,
    -- count when home team won a game when home_team_id = team_id
	COUNT(CASE WHEN g.home_team_id = gd.team_id and g.home_team_wins = 1 THEN 1 ELSE 0 END) AS win_count 
FROM game_details gd
LEFT JOIN games g ON gd.game_id = g.game_id
GROUP BY GROUPING SETS (
					(gd.player_name, gd.team_abbreviation, g.season),
					(gd.player_name, gd.team_abbreviation),
					(gd.player_name, g.season),
					(gd.team_abbreviation, g.season),
					(gd.team_abbreviation),
					()
					)
-- filtering overall from group set that may cause null value
having sum(gd.pts) <> 9999999 
order by scored desc
)
--- query for who scored the most points playing for one team
select player_name
from matches
-- filter groupset data focus on 1 team at all season(9999)
where player_name <> '(allplayer)' 
and team_abbreviation <> '(allteam)'
and season = 9999
order by scored desc
limit 1;

--- query for player who who scored the most points in one season
select player_name
from matches
-- filter only focus in one season for one player
where season <> 9999 and team_abbreviation <> '(allteam)'
order by scored desc
limit 1;

--query for team has won the most game
select team_abbreviation
from matches
-- filter focus on team that win the moust
where player_name <> '(allplayer)'
and team_abbreviation <> '(allteam)'
order by win_count desc
limit 1
;