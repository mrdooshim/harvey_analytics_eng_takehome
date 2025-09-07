WITH base AS (
  SELECT 
    CAST(id AS VARCHAR) AS firm_id,
    CAST(created AS DATE) AS created_date,
    CAST(firm_size AS INT) AS firm_size,
    CAST(arr_in_thousands AS INT) AS arr_in_thousands_usd
FROM {{ source('raw','firms') }}
  )
SELECT * FROM base;
