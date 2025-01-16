DROP TABLE players;
CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
    scoring_class scoring_class,
    years_since_last_season INTEGER,
    current_season INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY(player_name, current_season)
);

CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY(player_name, current_season)
);

WITH with_previous AS (
    SELECT player_name,
           current_season,
           scoring_class,
           is_active,
           LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
           LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
    FROM players_scd
),
with_indicators AS (
    SELECT *,
           CASE
               WHEN scoring_class <> previous_scoring_class THEN 1
               WHEN is_active <> previous_is_active THEN 1
               ELSE 0
           END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    SELECT *,
           SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM with_indicators
)

SELECT player_name,
       scoring_class,
       is_active,
       MIN(current_season) AS start_season,
       MAX(current_season) AS end_season,
	   2021 AS current_season
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY start_season;

WITH last_season_scd AS (
    SELECT * 
    FROM players_scd
    WHERE current_season = 2021
      AND end_season = 2021
),
historical_scd AS (
    SELECT * 
    FROM players_scd
    WHERE current_season = 2021
      AND end_season < 2021
),
this_season_data AS (
    SELECT * 
    FROM players
    WHERE current_season = 2022
),
unchanged_records AS (
    SELECT
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
    WHERE ts.scoring_class = ls.scoring_class
      AND ts.is_active = ls.is_active
)
SELECT * FROM unchanged_records