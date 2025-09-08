# A firm-level aggregate model combining firm metadata with usage and user metrics
# (e.g., active users, total queries, ARR). Useful for assessing firm-level health and segmentation.

WITH firm_profile AS (
  SELECT
    firm_id,
    firm_size,
    arr_in_thousands_usd, 
    created_date,
    date_diff('month', created_date, CURRENT_DATE) as tenure_months
  FROM {{ ref('stg_firms') }} 
),

firm_engagement AS (
  SELECT
    firm_id,
    COUNT(DISTINCT user_id) AS cnt_users,
    SUM(cnt_total_queries) AS cnt_queries,
    SUM(cnt_assistant_queries) AS cnt_assistant_queries, 
    SUM(cnt_vault_queries) AS cnt_vault_queries,
    SUM(cnt_workflow_queries) AS cnt_workflow_queries,
    ROUND(SUM(cnt_total_queries) / COUNT(DISTINCT user_id), 3) AS avg_queries_per_user,
  FROM {{ ref('user_engagement') }}
  GROUP BY 1
),
  
latest_complete_month_firm AS (
  SELECT
    firm_id,
    MAX(month) as latest_complete_month
  FROM {{ ref('user_engagement') }}
  WHERE month < CURRENT_DATE
  GROUP BY 1
),

latest_complete_month_summary AS (
  SELECT
    l.firm_id,
    l.latest_complete_month,
    SUM(ue.cnt_total_queries) AS cnt_total_queries_latest,
    SUM(ue.cnt_assistant_queries) AS cnt_assistant_queries_latest,
    SUM(ue.cnt_vault_queries) AS cnt_vault_queries_latest,
    SUM(ue.cnt_workflow_queries) AS cnt_workflow_queries_latest,
    SUM(ue.sum_docs) AS cnt_docs_uploaded_latest,
    COUNT(DISTINCT CASE WHEN ue.cnt_total_queries > 0 THEN ue.user_id END) AS cnt_active_users_latest_month,
    COUNT(DISTINCT ue.user_id) AS cnt_total_users,
    ROUND(SUM(ue.cnt_total_queries)/ NULLIF(COUNT(DISTINCT CASE WHEN ue.cnt_total_queries > 0 THEN ue.user_id END), 0), 3) AS queries_per_active_user_latest_month


  -- # of users by product usage frequency
    COUNT(DISTINCT CASE WHEN monthly_usage_days_level = 'inactive' THEN ue.user_id END) as num_inactive_usage_users,
    COUNT(DISTINCT CASE WHEN monthly_usage_days_level = 'rare' THEN ue.user_id END) as num_rare_usage_users,
    COUNT(DISTINCT CASE WHEN monthly_usage_days_level = 'mild' THEN ue.user_id END) as num_mild_usage_users,
    COUNT(DISTINCT CASE WHEN monthly_usage_days_level = 'frequent' THEN ue.user_id END) as num_frequent_usage_users,
    COUNT(DISTINCT CASE WHEN monthly_usage_days_level = 'very_frequent' THEN ue.user_id END) as num_very_frequent_usage_users,

  -- # of users by query volume level
    COUNT(DISTINCT CASE WHEN monthly_query_volume_level = 'none' THEN ue.user_id END) as num_no_query_volume_users,
    COUNT(DISTINCT CASE WHEN monthly_query_volume_level = 'low' THEN ue.user_id END) as num_low_query_volume_users,
    COUNT(DISTINCT CASE WHEN monthly_query_volume_level = 'moderate' THEN ue.user_id END) as num_moderate_query_volume_users,
    COUNT(DISTINCT CASE WHEN monthly_query_volume_level = 'high' THEN ue.user_id END) as num_high_query_volume_users,
    COUNT(DISTINCT CASE WHEN monthly_query_volume_level = 'very_high' THEN ue.user_id END) as num_very_high_query_volume_user
  
  FROM latest_complete_month_firm l
  JOIN  {{ ref('user_engagement') }} ue
    ON ue.firm_id = l.firm_id
    AND ue.month = l.latest_complete_month
  GROUP BY 1,2
)

SELECT 
  f.firm_id,
  f.firm_size,
  f.arr_in_thousands_usd,
  f.created_date,
  f.tenure_months,

  -- overall usage since inception
  fe.cnt_users,
  fe.cnt_queries,
  fe.cnt_assistant_queries,
  fe.cnt_vault_queries,
  fe.cnt_workflow_queries,
  fe.avg_queries_per_user,

  -- latest complete month snapshot
  l.latest_complete_month,
  l.cnt_total_queries_latest,
  l.cnt_assistant_queries_latest,
  l.cnt_vault_queries_latest,
  l.cnt_workflow_queries_latest,
  l.cnt_docs_uploaded_latest,
  l.cnt_active_users_latest_month,
  l.cnt_total_users_latest_month,
  l.queries_per_active_user_latest_month,

  -- latest month distributions
  l.num_inactive_usage_users,
  l.num_occasional_usage_users,
  l.num_weekly_usage_users,
  l.num_consistent_usage_users,
  l.num_habitual_usage_users,

  l.num_no_query_volume_users,
  l.num_low_query_volume_users,
  l.num_moderate_query_volume_users,
  l.num_high_query_volume_users,
  l.num_very_high_query_volume_users

FROM firm_profile f
LEFT JOIN firm_engagement fe
  ON fe.firm_id = f.firm_id
LEFT JOIN latest_complete_month_summary l
  ON l.firm_id = f.firm_id
;
