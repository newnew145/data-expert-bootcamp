SELECT 
	game_id,
    team_id,
    team_abbreviation,
    team_city,
    player_id,
    player_name,
    nickname,
    start_position,
    comment,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO" as trunovers,
    pf,
    pts,
    plus_minus
FROM (
	select *,
	row_number() over (partition by game_id,
									team_id,
									player_id
									) AS rn
	from game_details) dp
WHERE rn=1