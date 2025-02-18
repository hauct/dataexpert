-- Tạo bảng lưu trữ dữ liểu dạng SCD (Slowly Changing Dimension) Type 2
CREATE TABLE players_scd (
    player_name TEXT
  , scoring_class scoring_class  -- Phân loại khả năng ghi điểm
  , is_active BOOLEAN            -- Trạng thái hoạt động
  , start_season INTEGER         -- Mùa bắt đầu hiệu lực
  , end_season INTEGER           -- Mùa kết thúc hiệu lực
  , current_season INTEGER       -- Mùa hiện tại
  , PRIMARY KEY(player_name, start_season)  -- Khóa chính kết hợp
);

DO $$
DECLARE 
    min_season INTEGER := (SELECT MIN(season) FROM player_seasons);
    max_season INTEGER := (SELECT MAX(season) FROM player_seasons);
    current_season_var INTEGER;
BEGIN
    RAISE NOTICE 'Bắt đầu xử lý SCD Type 2 từ mùa % đến %', min_season, max_season;
    
    FOR current_season_var IN min_season..max_season LOOP
        RAISE NOTICE 'Đang xử lý mùa: %', current_season_var;
                
        -- Cập nhật SCD
        INSERT INTO players_scd
        WITH with_previous AS (
            SELECT 
                player_name,
                scoring_class,
                is_active,
                current_season,
                LAG(scoring_class) OVER w AS previous_scoring_class,
                LAG(is_active) OVER w AS previous_is_active
            FROM players
            WHERE current_season <= current_season_var
            WINDOW w AS (PARTITION BY player_name ORDER BY current_season)
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
                SUM(change_indicator) OVER w AS streak_identifier
            FROM with_indicators
            WINDOW w AS (PARTITION BY player_name ORDER BY current_season)
        )
        SELECT
            player_name,
            scoring_class,
            BOOL_AND(is_active) AS is_active, -- Đảm bảo trạng thái active nhất trong streak
            MIN(current_season) AS start_season,
            MAX(current_season) AS end_season,
            current_season_var AS current_season
        FROM with_streaks
        WHERE NOT EXISTS (
            SELECT 1 FROM players_scd 
            WHERE players_scd.player_name = with_streaks.player_name 
            AND players_scd.start_season = with_streaks.streak_identifier
        )
        GROUP BY player_name, streak_identifier, scoring_class, is_active;
        
        RAISE NOTICE 'Hoàn thành SCD cho mùa: %', current_season_var;
    END LOOP;
    
    RAISE NOTICE 'Xử lý SCD hoàn tất! Tổng số bản ghi: %', (SELECT COUNT(*) FROM players_scd);
END $$;


Sarunas Marciulionis, 1996

SELECT * FROM players_scd

WITH with_previous AS (SELECT 
	player_name,
	scoring_class,
	is_active,
	current_season,
	LAG(scoring_class) OVER w AS previous_scoring_class,
	LAG(is_active) OVER w AS previous_is_active
FROM players
WHERE current_season <= '1996' AND player_name = 'Sarunas Marciulionis'
WINDOW w AS (PARTITION BY player_name ORDER BY current_season)
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
		SUM(change_indicator) OVER w AS streak_identifier
	FROM with_indicators
	WINDOW w AS (PARTITION BY player_name ORDER BY current_season)
)
SELECT * FROM with_streaks;