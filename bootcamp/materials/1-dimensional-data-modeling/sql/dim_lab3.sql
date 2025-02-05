----------------------------------------------------------
-- PHẦN 1: ĐỊNH NGHĨA CẤU TRÚC DỮ LIỆU ĐỒ THỊ
----------------------------------------------------------

-- Tạo kiểu enum cho các loại node trong đồ thị
CREATE TYPE vertex_type AS ENUM(
    'player',  -- Đại diện cầu thủ
    'team',    -- Đại diện đội bóng
    'game'     -- Đại diện trận đấu
);

-- Bảng lưu trữ các node trong đồ thị
CREATE TABLE vertices (
    identifier TEXT,        -- Định danh duy nhất (VD: ID cầu thủ, ID trận đấu)
    type vertex_type,       -- Loại node (player/team/game)
    properties JSON,        -- Thuộc tính mở rộng dạng JSON
    PRIMARY KEY (identifier, type)  -- Khóa chính kết hợp
);

-- Bảng lưu trữ các mối quan hệ giữa các node
CREATE TABLE edges (
    subject_identifier TEXT,    -- Node nguồn
    subject_type vertex_type,   -- Loại node nguồn
    object_identifier TEXT,     -- Node đích 
    object_type vertex_type,    -- Loại node đích
    edge_type edge_type,        -- Loại quan hệ (VD: plays_against)
    properties JSON             -- Thuộc tính mở rộng của quan hệ
);

----------------------------------------------------------
-- PHẦN 2: THÊM DỮ LIỆU VÀO BẢNG VERTICES
----------------------------------------------------------

-- Thêm dữ liệu các trận đấu vào vertices
INSERT INTO vertices
SELECT
    game_id AS identifier,
    'game'::vertex_type AS type,
    json_build_object(  -- Đóng gói thông tin trận đấu
        'pts_home', pts_home,       -- Điểm đội nhà
        'pts_away', pts_away,       -- Điểm đội khách
        'winning_team', CASE WHEN home_team_wins = 1 
                            THEN home_team_id 
                            ELSE visitor_team_id END  -- Đội thắng
    ) as properties
FROM games;

-- Thêm dữ liệu cầu thủ vào vertices
INSERT INTO vertices
WITH players_agg AS (
    SELECT
        player_id AS identifier,
        MAX(player_name) AS player_name,      -- Tên cầu thủ
        COUNT(1) as number_of_games,          -- Số trận đã chơi
        SUM(pts) as total_points,             -- Tổng điểm
        ARRAY_AGG(DISTINCT team_id) AS teams  -- Danh sách các đội đã chơi
    FROM game_details
    GROUP BY player_id
)
SELECT
    identifier,
    'player'::vertex_type,
    json_build_object(  -- Đóng gói thông tin cầu thủ
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
    )
FROM players_agg;

-- Thêm dữ liệu đội bóng vào vertices (loại bỏ trùng lặp)
INSERT INTO vertices
WITH teams_deduped AS (
    SELECT *, 
    ROW_NUMBER() OVER(PARTITION BY team_id) as row_num  -- Đánh số bản ghi trùng
    FROM teams
)
SELECT
    team_id AS identifier,
    'team'::vertex_type AS type,
    json_build_object(  -- Đóng gói thông tin đội bóng
        'abbreviation', abbreviation,  -- Tên viết tắt
        'nickname', nickname,          -- Biệt danh
        'city', city,                  -- Thành phố
        'arena', arena,                -- Sân nhà
        'year_founded', yearfounded    -- Năm thành lập
    )
FROM teams_deduped
WHERE row_num = 1;  -- Chỉ lấy bản ghi đầu tiên của mỗi đội

----------------------------------------------------------
-- PHẦN 3: TẠO QUAN HỆ GIỮA CÁC NODE (EDGES)
----------------------------------------------------------

-- Thêm dữ liệu quan hệ giữa các cầu thủ
INSERT INTO edges
WITH deduped AS (
    SELECT *, 
           row_number() OVER (PARTITION BY player_id, game_id) AS row_number
    FROM game_details
    -- Loại bỏ bản ghi trùng lặp (1 cầu thủ/1 trận)
)
, filtered AS (
    SELECT * FROM deduped
    WHERE row_number = 1  -- Chỉ giữ bản ghi đầu tiên
)
, aggregated AS (
  SELECT
    f1.player_id as subject_player_id,
    f2.player_id AS object_player_id,
    CASE WHEN f1.team_abbreviation = f2.team_abbreviation
      THEN 'shares_team'::edge_type   -- Cùng đội
      ELSE 'plays_against'::edge_type  -- Đối đầu
    END as edge_type,
    MAX(f1.player_name) AS subject_player_name,
    MAX(f2.player_name) AS object_player_name,
    COUNT(1) AS num_games,            -- Số trận đối đầu/cùng đội
    SUM(f1.pts) AS subject_points,    -- Tổng điểm cầu thủ 1
    SUM(f2.pts) as object_points      -- Tổng điểm cầu thủ 2
  FROM filtered f1
  JOIN filtered f2
    ON f1.game_id = f2.game_id
    AND f1.player_name <> f2.player_name  -- Loại trừ tự join
    AND f1.player_id > f2.player_id  -- Tránh trùng lặp ngược (A-B và B-A)
  GROUP BY
    f1.player_id,
    f2.player_id,
    edge_type
)
SELECT
    subject_player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    object_player_id AS object_identifier,
    'player'::vertex_type AS object_type,
    edge_type AS edge_type,
    json_build_object(  -- Thông số về mối quan hệ
        'num_games', num_games,
        'subject_points', subject_points,
        'object_points', object_points
    )
FROM aggregated;

----------------------------------------------------------
-- PHẦN 4: TRUY VẤN PHÂN TÍCH DỮ LIỆU
----------------------------------------------------------

-- Truy vấn thống kê hiệu suất cầu thủ
SELECT
    v.properties->>'player_name' AS player_name,  -- Tên cầu thủ
    e.object_identifier AS opponent_id,           -- Đối thủ
    -- Tính tỷ lệ số trận trên điểm (tránh chia cho 0)
    CAST(v.properties->>'number_of_games' as REAL) /
    CASE WHEN CAST(v.properties->>'total_points' as REAL) = 0 THEN 1
         ELSE CAST(v.properties->>'total_points' as REAL) END AS game_point_ratio,
    e.properties->>'subject_points' AS player_points,  -- Điểm của cầu thủ
    e.properties->>'num_games' AS matchup_games        -- Số trận đối đầu
FROM vertices v
JOIN edges e
    ON v.identifier = e.subject_identifier
    AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;  -- Chỉ lấy quan hệ player-player
