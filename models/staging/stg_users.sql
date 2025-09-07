WITH base AS (
  SELECT
    CAST(id AS VARHCAR) AS user_id,
    CAST(created AS DATE) AS created_date,
    LOWER(TRIM(title)) AS title,
  FROM {{ source('raw','users') }}
  )
SELECT * FROM base;
