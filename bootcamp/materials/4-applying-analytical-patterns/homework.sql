--/Task 1
WITH player_season_changes AS (
  SELECT 
    player_name,
    current_season,
    is_active,
    LAG(is_active, 1) OVER (
        PARTITION BY player_name 
        ORDER BY current_season
    ) AS prev_is_active
  FROM players
)

SELECT
  player_name,
  current_season,
  CASE
    WHEN prev_is_active IS NULL THEN 'New'
    WHEN is_active = TRUE AND prev_is_active = FALSE THEN 'Returned from Retirement'
    WHEN is_active = TRUE AND prev_is_active = TRUE THEN 'Continued Playing'
    WHEN is_active = FALSE AND prev_is_active = TRUE THEN 'Retired'
    ELSE 'Stayed Retired'
  END AS status_change
FROM player_season_changes
WHERE player_name = 'Dominique Wilkins'
ORDER BY current_season;

--/Task 2
-- Tạo Common Table Expression (CTE) cơ sở
WITH game_stats AS (
  SELECT 
    player_name,
    team_abbreviation,
    RIGHT(game_id::TEXT, 4)::INT AS season, -- Giả sử 4 số cuối game_id là season
    pts,
    reb,
    ast,
    game_id
  FROM game_details
  WHERE min IS NOT NULL -- Loại bỏ các trường hợp DNP
)

SELECT 
  COALESCE(player_name, 'All Players') AS player,
  COALESCE(team_abbreviation, 'All Teams') AS team,
  COALESCE(season::TEXT, 'All Seasons') AS season,
  SUM(pts) AS total_points,
  COUNT(DISTINCT game_id) AS games_played,
  AVG(reb) AS avg_rebounds,
  GROUPING_ID(player_name, team_abbreviation, season) AS grouping_id
FROM game_stats
GROUP BY GROUPING SETS (
  (player_name, team_abbreviation),   -- Tổng hợp theo cầu thủ + đội
  (player_name, season),              -- Tổng hợp theo cầu thủ + mùa giải
  (team_abbreviation)                 -- Tổng hợp theo đội
)
ORDER BY grouping_id, total_points DESC;

--/Task 3
-- Câu 1: Chuỗi 90 trận thắng nhiều nhất
WITH team_wins AS (
  SELECT
    team_abbreviation,
    game_id,
    SUM(CASE WHEN plus_minus > 0 THEN 1 ELSE 0 END) AS win
  FROM game_details
  GROUP BY team_abbreviation, game_id
)

SELECT
  team_abbreviation,
  game_id,
  SUM(win) OVER (
    PARTITION BY team_abbreviation
    ORDER BY game_id
    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
  ) AS wins_in_90_games
FROM team_wins
ORDER BY wins_in_90_games DESC
LIMIT 1;

-- Câu 2: Chuỗi trận LeBron James ghi >10 điểm
WITH lebron_games AS (
  SELECT
    game_id,
    pts,
    CASE WHEN pts > 10 THEN 1 ELSE 0 END AS over_10_flag
  FROM game_details
  WHERE player_name = 'LeBron James'
)

SELECT
  game_id,
  pts,
  SUM(over_10_flag) OVER (
    ORDER BY game_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS consecutive_over_10
FROM lebron_games;
