-- Task 1
INSERT INTO fct_game_details
WITH deduped AS (
  SELECT
    g.game_date_est
    ,g.season
    ,g.home_team_id
    ,g.visitor_team_id
    ,gd.*
    ,ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id) AS row_num
  FROM game_details gd
  JOIN games g ON gd.game_id = g.game_id
)
SELECT
  game_date_est AS dim_game_date
  ,season AS dim_season
  ,team_id AS dim_team_id
  ,player_id AS dim_player_id
  ,player_name AS dim_player_name
  ,start_position AS dim_start_position
  --, Xác định có phải đội chủ nhà không bằng cách so sánh team_id
  ,team_id = home_team_id AS dim_is_playing_at_home
  --, Kiểm tra trạng thái không thi đấu (DNP/DND/NWT) từ trường comment
  ,COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play      --, DNP = Did Not Play
  ,COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress     --, DND = Did Not Dress
  ,COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_did_with_team     --, NWT = Not With Team
  --, Chuyển đổi định dạng phút từ "MM:SS" sang số thực (Ví dụ: "15:30" -> 15.5)
  ,CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL)/60 AS m_minutes 
  --, Các chỉ số thống kê cơ bản giữ nguyên từ bảng nguồn
  ,fgm AS m_fgm
  ,fga AS m_fga
  ,fg3m AS m_fg3m
  ,fg3a AS m_fg3a
  ,ftm AS m_ftm
  ,fta AS m_fta
  ,oreb AS m_oreb
  ,dreb AS m_dreb
  ,reb AS m_reb
  ,ast AS m_ast
  ,stl AS m_stl
  ,blk AS m_blk
  ,"TO" AS m_turnovers
  ,pf AS m_pf
  ,pts AS m_pts
  ,plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1;

-- Task 2
 CREATE TABLE users_cumulated (
     user_id BIGINT,
     dates_active DATE[],
     date DATE,
     PRIMARY KEY (user_id, date)
 );

SELECT * FROM devices

CREATE TABLE user_devices_cumulated (
 user_id BIGINT,
 device_activity_datelist JSONB,
 date DATE,
 PRIMARY KEY (user_id, date)
);


SELECT * FROM devices
WITH yesterday AS (
  SELECT *
  FROM users_cumulated
  WHERE date = DATE('2023-01-01')
),
today AS (
	SELECT user_id
	, jsonb_object_agg(browser_type, ARRAY[DATE(CAST(event_time AS TIMESTAMP))]) AS device_activity_datelist
	FROM events e
	JOIN devices d ON e.device_id = d.device_id
	WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
		AND user_id IS NOT NULL
	GROUP BY user_id
)

SELECT * FROM today WHERE user_id = '11355648956477300000'

SELECT * FROM events WHERE user_id = '11355648956477300000' AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')

SELECT DISTINCT browser_type FROM devices

SELECT user_id, COUNT(DISTINCT device_id) AS num_devices 
FROM events
WHERE  DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
GROUP BY user_id
ORDER BY num_devices DESC