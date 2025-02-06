CREATE TYPE films AS (
    film VARCHAR,
    votes INTEGER,
    rating NUMERIC,
    filmid VARCHAR,
    year INTEGER
);

CREATE TYPE quality_class AS ENUM(
    'star'
  , 'good'
  , 'average'
  , 'bad'
)

DROP TABLE actors;

-- Tạo bảng actors
CREATE TABLE actors (
    actorid VARCHAR,
    films film_struct[],
    quality_class VARCHAR,
	current_year INTEGER,
    is_active BOOLEAN,
	PRIMARY KEY(actorid, current_year)
);

WITH
yesterday AS (
    SELECT * 
    FROM actors 
    WHERE current_year = 1970 -- Năm trước
),
today AS (
    SELECT 
        actorid,
        ARRAY_AGG((film, votes, rating, filmid, year)::film_struct) AS films,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year = 1971 -- Năm hiện tại
    GROUP BY actorid, actor
)
INSERT INTO actors (actorid, films, quality_class, current_year, is_active)
SELECT
    COALESCE(y.actorid, t.actorid) AS actorid,
    CASE 
        WHEN y.films IS NULL THEN t.films
        ELSE y.films || t.films 
    END AS films,
    CASE
        WHEN t.avg_rating > 8 THEN 'star'::quality_class
        WHEN t.avg_rating > 7 THEN 'good'::quality_class
        WHEN t.avg_rating > 6 THEN 'average'::quality_class
        ELSE 'bad'::quality_class
    END AS quality_class,
    1971 AS current_year, -- Cập nhật năm hiện tại
    (t.actorid IS NOT NULL) AS is_active
FROM yesterday y
FULL OUTER JOIN today t 
    ON y.actorid = t.actorid;






--=================== OPTIMIZATION ===================
-- Tạo composite type và enum
CREATE TYPE film_struct AS (
    film VARCHAR,
    votes INTEGER,
    rating NUMERIC,
    filmid VARCHAR,
    year INTEGER
);

CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad');

-- Tạo bảng actors với cấu trúc tối ưu
CREATE TABLE IF NOT EXISTS actors (
    actorid VARCHAR,
    films film_struct[],
    quality_class quality_class,
    current_year INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY (actorid, current_year)
);

-- Tự động hóa toàn bộ quá trình tích lũy
DO $$
DECLARE
    prev_year INTEGER;
    curr_year INTEGER;
BEGIN
    -- Xác định năm xử lý tự động
    SELECT COALESCE(MAX(current_year), (SELECT MIN(year) FROM actor_films) - 1)
    INTO prev_year FROM actors;
    
    curr_year := prev_year + 1;

    -- Kiểm tra dữ liệu năm hiện tại có tồn tại
    IF NOT EXISTS (SELECT 1 FROM actor_films WHERE year = curr_year) THEN
        RAISE EXCEPTION 'No data for year %', curr_year;
    END IF;

    -- Thực hiện upsert với logic tự động
    WITH yesterday AS (
        SELECT * FROM actors 
        WHERE current_year = prev_year
    ),
    today AS (
        SELECT 
            actorid,
            ARRAY_AGG(DISTINCT (film, votes, rating, filmid, year)::film_struct) AS films,
            AVG(rating) AS avg_rating
        FROM actor_films
        WHERE year = curr_year
        GROUP BY actorid
    ),
    combined AS (
        SELECT
            COALESCE(y.actorid, t.actorid) AS actorid,
            CASE 
                WHEN y.films IS NULL THEN t.films
                ELSE (
                    SELECT ARRAY(
                        SELECT DISTINCT * 
                        FROM unnest(y.films || t.films)
                    )
                )
            END AS merged_films,
            COALESCE(t.avg_rating, y.avg_rating) AS avg_rating,
            t.actorid IS NOT NULL AS is_active
        FROM yesterday y
        FULL OUTER JOIN today t USING (actorid)
    )
    INSERT INTO actors (actorid, films, quality_class, current_year, is_active)
    SELECT
        actorid,
        merged_films,
        CASE
            WHEN avg_rating > 8 THEN 'star'::quality_class
            WHEN avg_rating > 7 THEN 'good'::quality_class
            WHEN avg_rating > 6 THEN 'average'::quality_class
            ELSE 'bad'::quality_class
        END,
        curr_year,
        is_active
    FROM combined
    ON CONFLICT (actorid, current_year)
    DO UPDATE SET
        films = EXCLUDED.films,
        quality_class = EXCLUDED.quality_class,
        is_active = EXCLUDED.is_active;

    -- Xóa dữ liệu năm trước nếu cần
    DELETE FROM actors 
    WHERE current_year = prev_year 
    AND actorid IN (SELECT actorid FROM actors WHERE current_year = curr_year);

    RAISE NOTICE 'Successfully processed year %', curr_year;
EXCEPTION
    WHEN others THEN
        RAISE WARNING 'Error processing year %: %', curr_year, SQLERRM;
        ROLLBACK;
END $$;