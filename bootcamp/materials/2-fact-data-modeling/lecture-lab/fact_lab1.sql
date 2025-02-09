--, Tạo bảng fact chứa chi tiết thống kê trận đấu của cầu thủ
CREATE TABLE fct_game_details (
	dim_game_date DATE                                                                  --, Ngày diễn ra trận đấu (định dạng DATE)
	,dim_season INTEGER                                                                 --, Mùa giải (dạng số nguyên)
	,dim_team_id INTEGER                                                                --, ID đội bóng (khóa ngoại tham chiếu đến bảng dim_team)
	,dim_player_id INTEGER                                                              --, ID cầu thủ (khóa ngoại tham chiếu đến bảng dim_player)
	,dim_player_name TEXT                                                               --, Tên cầu thủ (dạng text)
	,dim_start_position TEXT                                                            --, Vị trí bắt đầu trận đấu (Ví dụ: Guard, Forward,...)
	,dim_is_playing_at_home BOOLEAN                                                     --, Cờ xác định có đang thi đấu trên sân nhà không
	,dim_did_not_play BOOLEAN                                                           --, Cờ xác định cầu thủ không tham gia trận đấu (DNP)
	,dim_did_not_dress BOOLEAN                                                          --, Cờ xác định cầu thủ không có trong danh sách thi đấu (DND)
	,dim_not_with_team BOOLEAN                                                          --, Cờ xác định cầu thủ không còn thuộc đội (NWT)
	,m_minutes REAL                                                                     --, Số phút thi đấu (đổi từ chuỗi "MM:SS" sang dạng số thực)
	,m_fgm INTEGER                                                                      --, Số lần ném thành công (Field Goals Made)
	,m_fga INTEGER                                                                      --, Số lần ném thử (Field Goals Attempted)
	,m_fg3m INTEGER                                                                     --, Số lần ném 3 điểm thành công
	,m_fg3a INTEGER                                                                     --, Số lần ném 3 điểm thử
	,m_ftm INTEGER                                                                      --, Số lần ném phạt thành công (Free Throws Made)
	,m_fta INTEGER                                                                      --, Số lần ném phạt thử (Free Throws Attempted)
	,m_oreb INTEGER                                                                     --, Số rebound tấn công (Offensive)
	,m_dreb INTEGER                                                                     --, Số rebound phòng ngự (Defensive)
	,m_reb INTEGER                                                                      --, Tổng số rebound (ORE + DREB)
	,m_ast INTEGER                                                                      --, Số đường kiến tạo (Assists)
	,m_stl INTEGER                                                                      --, Số lần cướp bóng (Steals)
	,m_blk INTEGER                                                                      --, Số lần chặn bóng (Blocks)
	,m_turnovers INTEGER                                                                --, Số lần mất bóng (Turnovers)
	,m_pf INTEGER                                                                       --, Số lần phạm lỗi (Personal Fouls)
	,m_pts INTEGER                                                                      --, Điểm số (Points)
	,m_plus_minus INTEGER                                                               --, Chỉ số +/- (Hiệu số điểm khi cầu thủ trên sân)
	,PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)                            --, Khóa chính kết hợp 3 trường
);

--, Chèn dữ liệu vào bảng fact từ các bảng nguồn
INSERT INTO fct_game_details
WITH deduped AS (
  SELECT
    g.game_date_est
    ,g.season
    ,g.home_team_id
    ,g.visitor_team_id
    ,gd.*
    --, Đánh số thứ tự để loại bỏ bản ghi trùng lặp (ưu tiên bản ghi đầu tiên)
    ,ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id) AS row_num
  FROM game_details gd
  JOIN games g ON gd.game_id = g.game_id  --, Kết hợp bảng chi tiết và bảng thông tin trận đấu
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