WITH current_season AS (
  SELECT
    player_name,
    is_active,
    current_season,
    scoring_class,
	-- use row number dedup data if needed
    ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY current_season) AS rn
  FROM players_scd
),

state_transitions AS (
  SELECT
    curr.player_name,
    curr.current_season,
    curr.is_active AS is_active,
    CASE
      WHEN prev.is_active IS NULL AND curr.is_active = true THEN 'New'
      WHEN prev.is_active = true AND curr.is_active = false THEN 'Retired'
      WHEN prev.is_active = true AND curr.is_active = true THEN 'Continued Playing'
      WHEN prev.is_active = false AND curr.is_active = true THEN 'Returned from Retirement'
      WHEN prev.is_active = false AND curr.is_active = false THEN 'Stayed Retired'
      ELSE 'Other'
    END AS player_state_change
  FROM current_season curr
  -- left join to aggregate data or compared data with same rows
  LEFT JOIN current_season prev
    ON curr.player_name = prev.player_name
    -- compare previous and current season to get status
   AND curr.rn = prev.rn + 1
)

SELECT * FROM state_transitions
ORDER BY player_name, current_season;