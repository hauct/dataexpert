/* 1. Định nghĩa các kiểu dữ liệu tùy chỉnh */
-- Kiểu composite cho thống kê mùa giải
CREATE TYPE season_stats AS (
    season INTEGER  -- Năm mùa giải
  , gp INTEGER      -- Số trận đã chơi (Games Played)
  , pts REAL        -- Điểm trung bình mỗi trận (Points)
  , reb REAL        -- Rebounds trung bình
  , ast REAL        -- Hỗ trợ trung bình (Assists)
);

-- Kiểu enum phân loại khả năng ghi điểm
CREATE TYPE scoring_class AS ENUM (
    'star'    -- > 20 điểm/trận
  , 'good'    -- 15-20 điểm/trận 
  , 'average' -- 10-15 điểm/trận
  , 'bad'     -- <= 10 điểm/trận
);

/* 2. Tạo bảng players với các trường và ràng buộc */
CREATE TABLE players (
    player_name TEXT
  , height TEXT               -- Chiều cao cầu thủ
  , college TEXT              -- Trường đại học
  , country TEXT              -- Quốc tịch
  , draft_year TEXT           -- Năm được draft
  , draft_round TEXT          -- Vòng draft
  , draft_number TEXT         -- Thứ tự draft
  , season_stats season_stats[]  -- Mảng lưu lịch sử mùa giải
  , scoring_class scoring_class  -- Phân loại năng lực ghi điểm
  , years_since_last_season INTEGER  -- Số năm không thi đấu
  , current_season INTEGER    -- Mùa giải hiện tại
  , PRIMARY KEY(player_name, current_season)  -- Khóa chính composite
);

/* 3. Câu lệnh INSERT với logic cập nhật phức tạp */
INSERT INTO players
WITH 
-- Dữ liệu từ mùa trước (1995)
yesterday AS (
    SELECT * FROM players
    WHERE current_season = 1995
)
-- Dữ liệu mùa hiện tại (1996)
, today AS (
    SELECT * FROM player_seasons
    WHERE season = 1996
)

-- Kết hợp dữ liệu cũ và mới
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name  -- Tên cầu thủ
  , COALESCE(t.height, y.height) AS height                 -- Thông số chiều cao
  , COALESCE(t.college, y.college) AS college              -- Trường học
  , COALESCE(t.draft_year, y.draft_year) AS draft_year     -- Thông tin draft
  , COALESCE(t.draft_round, y.draft_round) AS draft_round
  , COALESCE(t.draft_number, y.draft_number) AS draft_number
  , CASE  -- Cập nhật mảng thống kê mùa giải
        WHEN y.season_stats IS NULL 
            THEN ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
        WHEN t.season IS NOT NULL 
            THEN y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
        ELSE y.season_stats
    END AS season_stats
  , CASE  -- Phân loại điểm
        WHEN t.season IS NOT NULL THEN
            CASE
                WHEN t.pts > 20 THEN 'star'::scoring_class
                WHEN t.pts > 15 THEN 'good'::scoring_class
                WHEN t.pts > 10 THEN 'average'::scoring_class
                ELSE 'bad'::scoring_class
            END
        ELSE y.scoring_class
    END AS scoring_class
  , CASE  -- Tính năm không thi đấu
        WHEN t.season IS NOT NULL THEN 0 
        ELSE y.years_since_last_season + 1 
    END AS years_since_last_season
  , COALESCE(t.season, y.current_season + 1) AS current_season  -- Cập nhật mùa

FROM today t
FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;

/* 4. Truy vấn mở rộng dữ liệu dạng mảng */
WITH unnested AS (
    SELECT 
        player_name
      , UNNEST(season_stats)::season_stats AS season_stats  -- Tách mảng thành các dòng
    FROM players
    WHERE current_season = 2001 
      AND player_name = 'Michael Jordan'
)