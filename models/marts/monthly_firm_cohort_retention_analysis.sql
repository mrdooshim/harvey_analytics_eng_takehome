-- Goal: Create a data model to analyze retention by firm-level signup cohort with cohort size, active firms, and retention rate
WITH firms AS (
  SELECT
    f.firm_id,
    DATE_TRUNC('month', f.created_date) AS cohort_month
  FROM {{ ref('stg_firms') }} f
  WHERE f.created_date IS NOT NULL
),

-- Firm's monthly activity from user_engagement
firm_month AS (
  SELECT
    ue.firm_id,
    ue.month AS activity_month,
    COUNT(DISTINCT CASE WHEN COALESCE(ue.cnt_total_queries, 0) > 0 THEN ue.firm_id END) AS active_firms_in_month,
    SUM(COALESCE(ue.cnt_total_queries, 0)) AS total_queries_in_month
  FROM {{ ref('user_engagement') }} ue
  GROUP BY 1,2
),

-- Only consider activity on/after the firm joined
firm_activity_valid AS (
  SELECT
    f.firm_id,
    f.cohort_month,
    fm.activity_month,
    fm.active_firms_in_month,
    fm.total_queries_in_month
  FROM firms f
  LEFT JOIN firm_month fm
    ON fm.firm_id = f.firm_id
   AND fm.activity_month >= f.cohort_month
),

-- Cohort sizes, i.e. number of firms that joined in each month
cohort_sizes AS (
  SELECT
    cohort_month,
    COUNT(DISTINCT firm_id) AS cohort_size
  FROM firms
  GROUP BY 1
),

-- Active firms per cohort by activity month
active_firms_by_cohort AS (
  SELECT
    fav.cohort_month,
    fav.activity_month,
    COUNT(DISTINCT CASE
      WHEN COALESCE(fav.active_firms_in_month, 0) >= 1
      THEN fav.firm_id END
    ) AS active_firms
  FROM firm_activity_valid fav
  WHERE fav.activity_month IS NOT NULL
  GROUP BY 1,2
),

final AS (
  SELECT
    a.cohort_month,
    a.activity_month,
    DATE_DIFF('month', a.cohort_month, a.activity_month) AS cohort_age_months,  -- 0 means the first month (should always be 100%)
    cs.cohort_size,
    COALESCE(a.active_firms, 0) AS active_firms,
    ROUND(1.0 * COALESCE(a.active_firms, 0) / NULLIF(cs.cohort_size, 0), 4) AS retention_rate
  FROM active_firms_by_cohort a
  JOIN cohort_sizes cs
    ON cs.cohort_month = a.cohort_month
)

SELECT *
FROM final
ORDER BY cohort_month, activity_month;
