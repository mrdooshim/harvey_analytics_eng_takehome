# ReadMe: Assumptions & Interpretation
Harvey Analytics Engineering Take-Home (Doo Shim)

#### Project Overview:
Build dbt models to analyze user- and firm-level engagement and retention metrics and better understand the health of the business/product.

#### General Assumptions & Notes
**_Business_**
- Since Harvey is a B2B SaaS product, I modeled cohorts at the firm/account-level rather than at the individual user level. That way, we can understand the health of the firm/account even if only a subset of users are active (e.g. Junior Associates using it daily while Partners rarely log in). Looking at the user-level alone could be useful for understanding individual workflows, but it could understate retention.
- While Feedback scores are valuable indicators of the perceived value of the outputs by users, I assumed it is an optional field, and did not factor them heavily into power user or engagement definitions beyond averaging when present.
  I treated `created_date` as the contract start date for calculating tenure. In practice, account creation and contract start dates may differ, but I simplified for this assignment.

**_Data_**
- The `users` table does not include an active/inactive flag, so I assumed all provisued users are live currently.
- The `events` table has one query per row but no unique `event_id` or `query_id`. In practice, we would want to track this and include a uniqueness test.
- `ARR` is modeled as a static field. In reality, expansion and contraction both occur, and a slowly-changing dimension table could be helpful for tracking this over time.
- Beyond `titles`, user roles are not available (e.g. Admin vs. Reader), but they're crucial for a more nuanced engagement analysis, as different product/access roles drive different usage patterns.
- Engagement categories are currently based on hardcoded thresholds (e.g. # of days using the product or # of queries submitted). But in the future, we could derive thresholds from distributions or percentiles, and likely segment more rigorously by firm size or user role so definitions are more scalable (e.g. a bigger firm likely has more queries to process per user vs. smaller firms).

#### Data Models
**_Staging_** (`models/staging/`)
- `stg_events` – standardized raw data with one row per query event
- `stg_firms` – standardized raw data with one row per firm
- `stg_users` – standardized raw data with one row per user

**Marts & Metrics** (`models/marts/`)
- `user_engagement` – one row per user per month with extend metrics including query counts, active days, docs uploaded, avg feedback scores, and engagement categorization based on historical behaviors
- `firm_usage_summary` – one row per firm with profile data (e.g. ARR, size, tenure) and usage metrics across all users across lifetime totals and the latest complete month snapshot
- `monthly_firm_cohort_retention_analysis` – firm-level monthly retention by created (joined) month; the model answers “Of the firms that joined in month X, how many remained active N months later?” 
