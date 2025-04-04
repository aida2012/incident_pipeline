WITH date_series AS (
    SELECT 
        DATE '2000-01-01' + n * INTERVAL '1 day' AS full_date
    FROM generate_series(0, 36889) AS t(n) -- 36889 días desde 2000 hasta 2100
),

date_attributes AS (
    SELECT 
        TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_id, 
        CAST(full_date AS DATE) AS full_date,
        EXTRACT(YEAR FROM full_date) AS year,
        EXTRACT(QUARTER FROM full_date) AS quarter,
        EXTRACT(MONTH FROM full_date) AS month,
        TO_CHAR(full_date, 'Month') AS month_name,
        EXTRACT(WEEK FROM full_date) AS week_of_year,
        EXTRACT(ISODOW FROM full_date) AS day_of_week, 
        TRIM(TO_CHAR(full_date, 'Day')) AS day_name, 
        CASE WHEN EXTRACT(ISODOW FROM full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CURRENT_TIMESTAMP AS insert_timestamp,
        CURRENT_TIMESTAMP AS update_timestamp
    FROM date_series
)

SELECT * FROM date_attributes
