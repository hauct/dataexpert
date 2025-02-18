CREATE TYPE season_stats AS (
    season INTEGER,
    gp INTEGER,
    pts REAL,
    reb REAL,
    ast REAL
);

CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');

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
    PRIMARY KEY(player_name, current_season)
);


DO $$
DECLARE 
    base_season INTEGER := 1996;  -- Mùa cơ sở đầu tiên
    current_season_var INTEGER;  -- Biến lưu mùa hiện tại
BEGIN
    -- Xác định mùa hiện tại dựa trên dữ liệu có sẵn
    SELECT COALESCE(MAX(current_season) + 1, base_season) 
    INTO current_season_var 
    FROM players;

    INSERT INTO players
    WITH yesterday AS (
        SELECT * FROM players 
        WHERE current_season = current_season_var - 1
    ),
    today AS (
        SELECT * FROM player_seasons 
        WHERE season = current_season_var
    )
    SELECT
        COALESCE(t.player_name, y.player_name),
        COALESCE(t.height, y.height),
        COALESCE(t.college, y.college),
        COALESCE(t.country, y.country),
        COALESCE(t.draft_year, y.draft_year),
        COALESCE(t.draft_round, y.draft_round),
        COALESCE(t.draft_number, y.draft_number),
        CASE
            WHEN y.season_stats IS NULL THEN ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
            WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
            ELSE y.season_stats
        END AS season_stats,
        CASE
            WHEN t.season IS NOT NULL THEN
                CASE
                    WHEN t.pts > 20 THEN 'star'::scoring_class
                    WHEN t.pts > 15 THEN 'good'::scoring_class
                    WHEN t.pts > 10 THEN 'average'::scoring_class
                    ELSE 'bad'::scoring_class
                END
            ELSE COALESCE(y.scoring_class, 'bad'::scoring_class)
        END AS scoring_class,
        CASE
            WHEN t.season IS NOT NULL THEN 0
            ELSE COALESCE(y.years_since_last_season, 0) + 1
        END AS years_since_last_season,
        (season IS NOT NULL) AS is_active ,
        current_season_var AS current_season  -- Sử dụng biến đã tính
    FROM today t
    FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;
END $$;

-- =====LOOP INSERT ALL DATA=====
DO $$
DECLARE 
    min_season INTEGER := (SELECT MIN(season) FROM player_seasons);
    max_season INTEGER := (SELECT MAX(season) FROM player_seasons);
    current_season_var INTEGER;
BEGIN
    RAISE NOTICE 'Bắt đầu xử lý từ mùa % đến %', min_season, max_season;
    
    FOR current_season_var IN min_season..max_season LOOP
        RAISE NOTICE 'Đang xử lý mùa: %', current_season_var;
        
        INSERT INTO players
        WITH yesterday AS (
            SELECT * FROM players 
            WHERE current_season = current_season_var - 1
        ),
        today AS (
            SELECT * FROM player_seasons 
            WHERE season = current_season_var
        )
        SELECT
            COALESCE(t.player_name, y.player_name),
            COALESCE(t.height, y.height),
            COALESCE(t.college, y.college),
            COALESCE(t.country, y.country),
            COALESCE(t.draft_year, y.draft_year),
            COALESCE(t.draft_round, y.draft_round),
            COALESCE(t.draft_number, y.draft_number),
            CASE
                WHEN y.season_stats IS NULL THEN ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
                WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
                ELSE y.season_stats
            END AS season_stats,
            CASE
                WHEN t.season IS NOT NULL THEN
                    CASE
                        WHEN t.pts > 20 THEN 'star'::scoring_class
                        WHEN t.pts > 15 THEN 'good'::scoring_class
                        WHEN t.pts > 10 THEN 'average'::scoring_class
                        ELSE 'bad'::scoring_class
                    END
                ELSE COALESCE(y.scoring_class, 'bad'::scoring_class)
            END AS scoring_class,
            CASE
                WHEN t.season IS NOT NULL THEN 0
                ELSE COALESCE(y.years_since_last_season, 0) + 1
            END AS years_since_last_season,
            (season IS NOT NULL) AS is_active ,
            current_season_var AS current_season
        FROM today t
        FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;
        
        RAISE NOTICE 'Hoàn thành mùa: %', current_season_var;
    END LOOP;
    
    RAISE NOTICE 'Xử lý hoàn tất!';
END $$;