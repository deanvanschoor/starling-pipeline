-- Create Date Dimension Table in DuckDB
CREATE OR REPLACE TABLE stg.dim_date AS
WITH date_spine AS (
    SELECT 
        DATE '2020-01-01' + INTERVAL (seq) DAY AS date_key
    FROM generate_series(0, 4017) AS t(seq)  -- 11 years of dates (2020-2030)
)
SELECT
    -- Primary Key
    date_key,
    
    -- Date Components
    EXTRACT(YEAR FROM date_key) AS year,
    EXTRACT(MONTH FROM date_key) AS month,
    EXTRACT(DAY FROM date_key) AS day,
    EXTRACT(QUARTER FROM date_key) AS quarter,
    EXTRACT(WEEK FROM date_key) AS week_of_year,
    DAYOFWEEK(date_key) AS day_of_week,  -- 0=Sunday, 6=Saturday
    DAYOFYEAR(date_key) AS day_of_year,
    
    -- Formatted Dates
    STRFTIME(date_key, '%Y-%m-%d') AS date_string,
    STRFTIME(date_key, '%Y%m%d') AS date_int,
    
    -- Month Names and Abbreviations
    MONTHNAME(date_key) AS month_name,
    STRFTIME(date_key, '%b') AS month_abbr,
    STRFTIME(date_key, '%Y-%m') AS year_month,
    
    -- Day Names and Abbreviations
    DAYNAME(date_key) AS day_name,
    STRFTIME(date_key, '%a') AS day_abbr,
    
    -- Quarter Information
    'Q' || EXTRACT(QUARTER FROM date_key) AS quarter_name,
    EXTRACT(YEAR FROM date_key) || '-Q' || EXTRACT(QUARTER FROM date_key) AS year_quarter,
    
    -- Week Information
    'W' || LPAD(CAST(EXTRACT(WEEK FROM date_key) AS VARCHAR), 2, '0') AS week_name,
    EXTRACT(YEAR FROM date_key) || '-W' || LPAD(CAST(EXTRACT(WEEK FROM date_key) AS VARCHAR), 2, '0') AS year_week,
    
    -- Fiscal Year (assuming fiscal year starts in July - adjust as needed)
    CASE 
        WHEN EXTRACT(MONTH FROM date_key) >= 7 
        THEN EXTRACT(YEAR FROM date_key) + 1 
        ELSE EXTRACT(YEAR FROM date_key) 
    END AS fiscal_year,
    
    CASE 
        WHEN EXTRACT(MONTH FROM date_key) BETWEEN 7 AND 9 THEN 1
        WHEN EXTRACT(MONTH FROM date_key) BETWEEN 10 AND 12 THEN 2
        WHEN EXTRACT(MONTH FROM date_key) BETWEEN 1 AND 3 THEN 3
        ELSE 4
    END AS fiscal_quarter,
    
    -- Boolean Flags
    CASE WHEN DAYOFWEEK(date_key) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN DAYOFWEEK(date_key) NOT IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekday,
    
    -- First and Last Day Flags
    CASE WHEN EXTRACT(DAY FROM date_key) = 1 THEN TRUE ELSE FALSE END AS is_first_day_of_month,
    CASE WHEN date_key = LAST_DAY(date_key) THEN TRUE ELSE FALSE END AS is_last_day_of_month,
    
    CASE 
        WHEN date_key = DATE_TRUNC('quarter', date_key) 
        THEN TRUE 
        ELSE FALSE 
    END AS is_first_day_of_quarter,
    
    CASE 
        WHEN date_key = LAST_DAY(DATE_TRUNC('quarter', date_key) + INTERVAL '2 months') 
        THEN TRUE 
        ELSE FALSE 
    END AS is_last_day_of_quarter,
    
    CASE 
        WHEN EXTRACT(MONTH FROM date_key) = 1 AND EXTRACT(DAY FROM date_key) = 1 
        THEN TRUE 
        ELSE FALSE 
    END AS is_first_day_of_year,
    
    CASE 
        WHEN EXTRACT(MONTH FROM date_key) = 12 AND EXTRACT(DAY FROM date_key) = 31 
        THEN TRUE 
        ELSE FALSE 
    END AS is_last_day_of_year,
    
    -- Relative Date Calculations
    DATE_TRUNC('month', date_key) AS first_day_of_month,
    LAST_DAY(date_key) AS last_day_of_month,
    DATE_TRUNC('quarter', date_key) AS first_day_of_quarter,
    DATE_TRUNC('year', date_key) AS first_day_of_year,
    

FROM date_spine
ORDER BY date_key;