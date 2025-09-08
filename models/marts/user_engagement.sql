# One row per user per month, including usage metrics
# (e.g., query counts, last active date, query types, engagement_level, etc.).
# Use this model to evaluate user behavior over time.

WITH date_spine AS (
  SELECT 
    u.user_id,
    u.firm_id,
    m.month
  FROM (SELECT DISTINCT user_id, firm_id FROM {{ ref('stg_users') }}) u
  CROSS JOIN (
    SELECT DISTINCT DATE_TRUNC('month', created_date) AS month
    FROM {{ ref('stg_events') }}
  ) m
),
  
events AS (
  SELECT 
    e.user_id,
    u.firm_id,
    DATE_TRUNC('month', e.created_date) AS month,
    e.created_date,
    e.event_type,
    e.num_docs,
    e.feedback_score
  FROM {{ ref('stg_events') }} e
  LEFT JOIN {{ ref('stg_users') }} u 
    ON u.user_id = e.user_id
),

monthly_user_aggr AS (
  SELECT
    user_id,
    firm_id,
    month,
    COUNT(*) AS cnt_total_queries,
    COUNT(DISTINCT created_date) AS cnt_active_days_in_month,
    MIN(created_date) AS first_active_date_in_month,
    MAX(created_date) AS last_active_date_in_month,
    AVG(feedback_score) AS avg_feedback_score,
    SUM(COALESCE(num_docs, 0)) AS sum_docs
  FROM events
  GROUP BY 1,2,3
),
  
monthly_user_activity_type AS (
  SELECT
    user_id,
    firm_id,
    month,
    SUM(CASE WHEN event_type = 'assistant' THEN 1 ELSE 0 END) AS cnt_assistant_queries,
    SUM(CASE WHEN event_type = 'vault' THEN 1 ELSE 0 END) AS cnt_vault_queries,
    SUM(CASE WHEN event_type = 'workflow' THEN 1 ELSE 0 END) AS cnt_workflow_queries
  FROM events 
  GROUP BY 1,2,3
),

final AS (
  SELECT 
    s.user_id,
    s.firm_id,
    s.month,
    COALESCE(a.cnt_total_queries, 0) AS cnt_total_queries,
    COALESCE(a.cnt_active_days_in_month, 0) AS cnt_active_days_in_month,
    COALESCE(a.sum_docs, 0) AS sum_docs,
    COALESCE(t.cnt_assistant_queries, 0) AS cnt_assistant_queries,
    COALESCE(t.cnt_vault_queries, 0) AS cnt_vault_queries,
    COALESCE(t.cnt_workflow_queries, 0) AS cnt_workflow_queries,
    ROUND(cnt_total_queries / cnt_active_days_in_month, 3) queries_per_active_day,

  -- engagement level categorization
    CASE
      WHEN active_days_in_month = 0 THEN 'inactive'
      WHEN active_days_in_month BETWEEN 1 AND 4 THEN 'rare'
      WHEN active_days_in_month BETWEEN 5 AND 8 THEN 'mild'
      WHEN active_days_in_month BETWEEN 9 AND 12 THEN 'frequent'
      WHEN active_days_in_month >= 13 THEN 'very_frequent'
    END AS monthly_usage_days_level,
  
    CASE
      WHEN cnt_total_queries = 0 THEN 'none'
      WHEN cnt_total_queries BETWEEN 1 AND 9 THEN 'low'
      WHEN cnt_total_queries BETWEEN 10 AND 24 THEN 'moderate'
      WHEN cnt_total_queries BETWEEN 25 AND 39 THEN 'high'
      WHEN cnt_total_queries >= 40 THEN 'very_high'
    END AS monthly_query_volume_level
  
  FROM date_spine s
  LEFT JOIN monthly_user_aggr a 
    ON a.month = s.month
    AND a.user_id = s.user_id
    AND a.firm_id = s.firm_id
  LEFT JOIN monthly_user_activity_type t
    ON t.month = s.month
    AND t.user_id = s.user_id
    AND t.firm_id = s.firm_id
)

SELECT * FROM final;
