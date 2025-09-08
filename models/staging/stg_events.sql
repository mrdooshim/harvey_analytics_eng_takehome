WITH base AS (
  SELECT
  CAST(created AS TIMESTAMP) AS created,
  CAST(firm_id AS VARCHAR) AS firm_id,
  CAST(user_id AS VARCHAR) AS user_id,
  UPPER(event_type) AS event_type,
  CAST(num_docs AS INT) AS num_docs,
  CAST(feedback_score AS INT) AS feedback_score
FROM {{ source('raw','events') }}
  )
SELECT * FROM base;
