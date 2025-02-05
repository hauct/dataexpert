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

-- Cập nhật dữ liệu SCD từ bảng players
INSERT INTO players_scd
WITH 
-- Bước 1: Lấy dữ liệu từ mùa trước
with_previous AS (
    SELECT 
        player_name
      , current_season
      , scoring_class
      , is_active
      , LAG(scoring_class, 1) OVER (
            PARTITION BY player_name ORDER BY current_season
        ) AS previous_scoring_class  -- Phân loại điểm mùa trước
      , LAG(is_active, 1) OVER (
            PARTITION BY player_name ORDER BY current_season
        ) AS previous_is_active  -- Trạng thái hoạt động mùa trước
    FROM players
    WHERE current_season <= 2021  -- Lọc dữ liệu lịch sử
),

-- Bước 2: Đánh dấu các thay đổi
with_indicators AS (
    SELECT *
      , CASE
            WHEN scoring_class <> previous_scoring_class THEN 1  -- Đánh dấu thay đổi phân loại
            WHEN is_active <> previous_is_active THEN 1          -- Đánh dấu thay đổi trạng thái
            ELSE 0 
        END AS change_indicator  -- Chỉ số phát hiện thay đổi
    FROM with_previous
),

-- Bước 3: Xác định các giai đoạn không đổi (streaks)
with_streaks AS (
    SELECT *
      , SUM(change_indicator) OVER (
            PARTITION BY player_name ORDER BY current_season
        ) AS streak_identifier  -- Định danh nhóm các mùa không thay đổi
    FROM with_indicators
)

-- Bước 4: Tổng hợp kết quả
SELECT 
    player_name
  , scoring_class
  , is_active
  , MIN(current_season) AS start_season  -- Mùa bắt đầu của streak
  , MAX(current_season) AS end_season    -- Mùa kết thúc của streak
  , 2021 AS current_season  -- Gán mùa hiện tại
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY start_season;

-- Tạo kiểu dữ liệu cho SCD
CREATE TYPE scd_type AS (
    scoring_class scoring_class  -- Phân loại điểm
  , is_active BOOLEAN            -- Trạng thái hoạt động
  , start_season INTEGER         -- Mùa bắt đầu
  , end_season INTEGER           -- Mùa kết thúc
);

-- Cập nhật SCD cho mùa mới (2022)
WITH 
-- Dữ liệu SCD mùa trước (2021)
last_season_scd AS (
    SELECT * 
    FROM players_scd
    WHERE current_season = 2021 AND end_season = 2021  -- Chỉ lấy bản ghi cuối cùng
),

-- Dữ liệu lịch sử trước 2021
historical_scd AS (
    SELECT 
        player_name
      , scoring_class
      , is_active
      , start_season
      , end_season
    FROM players_scd
    WHERE current_season = 2021 AND end_season < 2021  -- Lọc các bản ghi cũ
),

-- Dữ liệu mùa hiện tại (2022)
this_season_data AS (
    SELECT *
    FROM players
    WHERE current_season = 2022  -- Lấy dữ liệu mới nhất
),

-- Các bản ghi không thay đổi từ mùa trước
unchanged_records AS (
    SELECT
        ts.player_name
      , ts.scoring_class
      , ts.is_active
      , ls.start_season  -- Giữ nguyên start_season
      , ts.current_season AS end_season  -- Cập nhật end_season
    FROM this_season_data ts
    JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
    WHERE ts.scoring_class = ls.scoring_class
      AND ts.is_active = ls.is_active  -- Điều kiện không thay đổi
),

-- Các bản ghi có thay đổi
changed_records AS (
    SELECT
        ts.player_name
      , ts.scoring_class
      , ts.is_active
      , ls.start_season
      , ts.current_season AS end_season
      -- Tạo 2 bản ghi: đóng bản ghi cũ và mở bản ghi mới
      , UNNEST(ARRAY[
            ROW(ls.scoring_class, ls.is_active, ls.start_season, ls.end_season)::scd_type,  -- Bản ghi cũ
            ROW(ts.scoring_class, ts.is_active, ts.current_season, ts.current_season)::scd_type  -- Bản ghi mới
        ]) AS records
    FROM this_season_data ts
    JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
    WHERE ts.scoring_class <> ls.scoring_class
      OR ts.is_active <> ls.is_active  -- Điều kiện phát hiện thay đổi
      OR ls.player_name IS NULL  -- Xử lý trường hợp mới
),

-- Mở rộng các bản ghi đã thay đổi
unnest_changed_records AS (
    SELECT 
        player_name
      , (records::scd_type).scoring_class
      , (records::scd_type).is_active
      , (records::scd_type).start_season
      , (records::scd_type).end_season
    FROM changed_records
),

-- Các bản ghi hoàn toàn mới
new_records AS (
    SELECT 
        ts.player_name
      , ts.scoring_class
      , ts.is_active
      , ts.current_season AS start_season  -- Bắt đầu từ mùa hiện tại
      , ts.current_season AS end_season    -- Kết thúc ở mùa hiện tại
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
        ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL  -- Xác định bản ghi mới
)

-- Kết hợp tất cả các bản ghi
SELECT * FROM historical_scd  -- Giữ nguyên lịch sử
UNION ALL
SELECT * FROM unnest_changed_records  -- Thêm bản ghi thay đổi
UNION ALL
SELECT * FROM new_records;  -- Thêm bản ghi mới