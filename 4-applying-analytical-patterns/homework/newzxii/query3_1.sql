-- query for most game win by team in 90 days stretch

WITH win_by_date AS(
SELECT match_date, team_abbreviation, win_flag, row_number() over(partition by f.team_abbreviation order by f.match_date) as row_flag
FROM (
SELECT 
	g.game_date_est as match_date,
	COALESCE(gd.team_abbreviation,'(allteam)') as team_abbreviation,
	CASE WHEN g.home_team_id = gd.team_id and g.home_team_wins = 1 THEN 1 ELSE 0 END AS win_flag,
	row_number() over (PARTITION BY team_abbreviation, g.game_date_est ORDER BY g.game_date_est) as rn
FROM game_details gd
LEFT JOIN games g ON gd.game_id = g.game_id
order by match_date desc) f
-- filter duplicate row from player in team
WHERE rn = 1
),
summary_tbl AS (
select cur.team_abbreviation, cur.row_flag AS start_flg,
-- use row between to sum up between 90 rows (day)
sum(cur.win_flag) OVER (partition by cur.team_abbreviation ROWS BETWEEN 90 PRECEDING AND CURRENT ROW)AS win_cnt
from win_by_date cur
group by cur.team_abbreviation, cur.row_flag , cur.win_flag
)
SELECT max(win_cnt) as most_win_in_90_days
from summary_tbl