WITH base AS (
  SELECT
  CAST(created AS timestamp) AS created,
  CAST(firm_id AS varchar) AS firm_id,
  CAST(user_id AS varchar) AS user_id,
  lower(trim(event_type)) AS event_type,
  CAST(num_docs AS int) AS num_docs,
  CAST(feedback_score AS int) AS feedback_score,
FROM {{ source('raw','events') }}
  )
SELECT * FROM base;
