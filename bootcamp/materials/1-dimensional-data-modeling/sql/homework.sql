DROP TABLE actors;

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
);

-- Tạo bảng actors
CREATE TABLE actors (
    actorid VARCHAR,
    films films[],
    quality_class quality_class,
	current_year INTEGER,
    is_active BOOLEAN,
	PRIMARY KEY(actorid, current_year)
);

WITH
yesterday AS (
    SELECT * 
    FROM actors 
    WHERE current_year = 1971 -- Năm trước
),
today AS (
    SELECT 
        actorid,
        ARRAY_AGG((film, votes, rating, filmid, year)::films) AS films,
		year,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year = 1972 -- Năm hiện tại
    GROUP BY actorid, year
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
    1972 AS current_year, -- Cập nhật năm hiện tại
    (t.year IS NOT NULL) AS is_active
FROM yesterday y
FULL OUTER JOIN today t 
    ON y.actorid = t.actorid;

-- Tạo bảng lưu trữ dữ liểu dạng SCD (Slowly Changing Dimension) Type 2
CREATE TABLE actors_history_scd (
    actorid TEXT
  , quality_class quality_class
  , is_active BOOLEAN
  , start_year INTEGER
  , end_year INTEGER
  , current_year INTEGER
  , PRIMARY KEY(actorid, start_year)
);

CREATE TYPE scd_type AS (
	quality_class quality_class,
	is_active boolean,
	start_year INTEGER,
	end_year INTEGER
)

WITH last_year_scd AS(
    SELECT * FROM actors_history_scd
    WHERE current_year = 1970
    AND end_year = 1970
)
, historical_scd AS (
	SELECT
		actorid,
	    quality_class,
	    is_active,
	    start_year,
	    end_year
	FROM actors_history_scd
	WHERE start_year = 1970
	AND end_year < 1970
)
, this_year_data AS (
	SELECT
		*
	FROM actors
	WHERE current_year = 1970
)
, unchanged_records AS (
	SELECT
		ty.actorid,
		ty.quality_class,
		ty.is_active,
		ly.start_year,
		ty.current_year AS end_year
	FROM this_year_data ty
	JOIN last_year_scd ly
	ON ty.actorid = ly.actorid
	WHERE ty.quality_class = ly.quality_class AND ty.is_active = ly.is_active
)
, changed_records AS (
	SELECT
		ty.actorid,
		UNNEST(ARRAY[
			ROW(
				ly.quality_class,
				ly.is_active,
				ly.start_year,
				ly.end_year
			)::scd_type,
			ROW(
				ty.quality_class,
				ty.is_active,
				ty.current_year,
				ty.current_year
			)::scd_type
		]) AS records
		FROM this_year_data ty
		LEFT JOIN last_year_scd ly
		ON ty.actorid = ly.actorid
		WHERE (ty.quality_class <> ly.quality_class) AND (ty.is_active <> ly.is_active)
)
, unnested_changed_records AS (
	SELECT actorid,
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_year,
		(records::scd_type).end_year
	FROM changed_records
)

SELECT * FROM unnested_changed_records