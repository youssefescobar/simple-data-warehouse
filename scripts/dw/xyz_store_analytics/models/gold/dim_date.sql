WITH date_series AS (
    SELECT CAST(gs AS DATE) AS date_day
    FROM generate_series(
        '2020-01-01'::timestamp,
        '2026-12-31'::timestamp,
        '1 day'::interval
    ) AS gs
)

SELECT
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) AS date_key,
    date_day AS date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Month') AS month_name,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(ISODOW FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_name,
    CASE 
        WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN TRUE
        ELSE FALSE 
    END AS is_weekend
FROM date_series
ORDER BY date_key