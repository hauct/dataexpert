--, Xóa bảng cũ nếu tồn tại để tạo mới
DROP TABLE users_cumulated;

--, Tạo bảng lưu trữ dữ liệu hoạt động tích lũy của người dùng
CREATE TABLE users_cumulated (
	user_id TEXT                                                                        --, ID người dùng (dạng text)
	,dates_active DATE[]                                                                --, Mảng ngày hoạt động của người dùng
	,date DATE                                                                          --, Ngày cập nhật dữ liệu (dùng trong khóa chính)
	,PRIMARY KEY(user_id, date)                                                         --, Khóa chính kết hợp user_id và date
);

--, Cập nhật dữ liệu tích lũy bằng kỹ thuật Slowly Changing Dimension (SCD) type 2
INSERT INTO users_cumulated
WITH 
--, Dữ liệu từ ngày hôm trước (2023-01-30)
yesterday AS (
  SELECT *
  FROM users_cumulated
  WHERE date = DATE('2023-01-30')                                                       --, Lọc dữ liệu cũ theo ngày
),

--, Dữ liệu hoạt động hôm nay (2023-01-31)
today AS (
  SELECT 
    CAST(user_id AS TEXT)                                                               --, Đảm bảo định dạng text cho user_id
    ,DATE(CAST(event_time AS TIMESTAMP)) AS date_active                                 --, Chuẩn hóa ngày hoạt động từ event_time
  FROM events
  WHERE 
    DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')                            --, Lọc sự kiện trong ngày 31/01
    AND user_id IS NOT NULL                                                             --, Loại bỏ bản ghi không có user_id
  GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))                                 --, Gom nhóm theo user và ngày
)

--, Kết hợp dữ liệu cũ và mới
SELECT 
  COALESCE(t.user_id, y.user_id) AS user_id                                             --, Ưu tiên user_id từ dữ liệu mới nhất
  ,CASE 
    WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]                               --, Người dùng mới: Tạo mảng ngày hoạt động
    WHEN t.date_active IS NULL THEN y.dates_active                                      --, Người dùng không hoạt động: Giữ nguyên lịch sử
    ELSE ARRAY[t.date_active] || y.dates_active                                         --, Cập nhật: Thêm ngày mới vào đầu mảng
  END AS dates_active                                                                   --, Kết quả mảng ngày hoạt động
  ,COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date                           --, Ngày cập nhật logic (+1 day khi không có hoạt động)
FROM today t
FULL OUTER JOIN yesterday y                                                             --, Kết hợp toàn bộ dữ liệu cũ và mới
ON t.user_id = y.user_id;

--, Phần tính toán trạng thái hoạt động bằng kỹ thuật bitmask
WITH 
--, Lấy dữ liệu đã tích lũy đến ngày 31/01
users AS (
  SELECT * 
  FROM users_cumulated
  WHERE date = DATE('2023-01-31')                                                       --, Lọc dữ liệu mới nhất
),

--, Tạo chuỗi ngày trong tháng 01/2023 (từ 02/01 đến 31/01)
series AS (
  SELECT * 
  FROM generate_series(
    DATE('2023-01-02'),                                                                 --, Ngày bắt đầu
    DATE('2023-01-31'),                                                                 --, Ngày kết thúc
    '1 day'                                                                             --, Bước nhảy 1 ngày
  ) AS series_date
),

--, Tính toán giá trị bitmask đại diện cho từng ngày
place_holder_ints AS (
  SELECT 
    CASE 
      WHEN dates_active @> ARRAY[DATE(series_date)]                                     --, Kiểm tra ngày có trong mảng hoạt động
        THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)                    --, Tạo giá trị bit tương ứng (ví dụ: 2^31 cho ngày 02/01)
      ELSE 0                                                                            --, Giá trị 0 nếu không hoạt động
    END AS placeholder_int_value                                                        --, Kết quả giá trị bitmask
    ,*
  FROM users 
  CROSS JOIN series                                                                     --, Kết hợp mọi tổ hợp user x ngày
)

--, Tổng hợp kết quả cuối cùng
SELECT 
  user_id
  ,CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))                          --, Chuyển tổng bitmask thành dạng nhị phân 32-bit
  ,BIT_COUNT(                                                                           --, Đếm số bit 1
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
  ) > 0 AS dim_is_monthly_active                                                        --, Cờ hoạt động trong tháng (có ít nhất 1 ngày hoạt động)
  ,BIT_COUNT(                                                                           --, Kiểm tra hoạt động 7 ngày gần nhất
    CAST('11111110000000000000000000000000' AS BIT(32)) &                               --, Mask 7 bit đầu (tương ứng 7 ngày cuối)
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
  ) > 0 AS dim_is_weekly_active                                                         --, Cờ hoạt động trong tuần
FROM place_holder_ints
GROUP BY user_id;