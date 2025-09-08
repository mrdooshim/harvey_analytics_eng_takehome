# Analytics Questions

**1. Based on your user_engagement model, how would you define a power user?**
- A "power user" is someone with consistently high engagement in a given month. In this exercise, I categorized users by both # of active days and query volume in the month, where a user is labeled:
  - a) Frequency (# of Days Active Per Month, i.e. # of days where the user submitted at least one query)
    - Very frequent: ≥13 active days (~4 days/week)
    - Frequent: 9–12 active days (~3 days/week)
    - Mild: 5–8 active days (~2 days/week)
    - Rare: 1–4 active days (~1 day/week)
    - Inactive: 0 active days
  - b) Query Volume (# of Queries Per Month):
    - Very high: >=40 queries
    - High: 25–39 queries
    - Moderate: 10–24 queries
    - Low: 1–9 queries
    - None: 0 queries

I would define a "power user" as someone who falls into the top tiers of both frequency and volume – engaging with the prodcut multiple times per week **_and_** submitting a high number of queries. This indicates the stickiness to the product and that Harvey is deeply embedded in their workflow.

That said, with contextual refinements, we can get one layer deeper in rigor of defining power user in the following ways:
- Engagement expectations by role – e.g. users with the "Junior Associate" title and other individual contributor titles may more likely be daily users, while Partners may only log in to the product occasionally. The thresholds above could be tuned dynamically by job title or company size segment.
- It may also be useful to segment by workflow types – not all events reflect "core" product use equally, so I would align internally on which events (WORKFLOW vs. VAULT vs. ASSISTANT) best represent the product's intended value and core action that indicates user value.

**2. What potential issues or data quality concerns does the data surface? (These could be anomalies, missing data, inconsistent definitions, etc.)**

i) Events Table
- Missing unique identifier: There is currently no `event_id` or `query_id` field present. We are simply assuming that each row is a unique event, but in production, each event should have a stable unique key for de-duplication, debugging, and joining to downstream fact tables. Additionally, we'd want dbt tests for `unique` and `not_null`.
- Feedback score range/quality: The valid range (e.g. 1-5) is not documented, so the data end-user might misinterpret outliers or treat the field as continuous when it is categorical without clear definitions.
- Event Type Classification: As mentioned above, not all events are created equal – there are certain queries/event types that represent "core engagement" vs. peripheral activity, and the core events are important for retention/engagement metrics.

ii) Users table
- Firm ID field mising: The raw `users` table doesn't include `firm_id`, which makes it impossible to directly tie users to their firms without relying on the `events` table. This could result in bias in firm metrics, e.g. active users per firm might be understated, since inactive users (who count towards billing/seats) won't be captured. This may also impact downstream models that depend on a firm-user relationship.

iii) Firms table
- Static ARR: ARR field is currently a single snapshot value. But in reality, ARR changes with expansions or contractions. Ideally, we should have a slowly changing dimension that captures these changes for better historical analysis.
- Contract vs. Created_date: In this dataset, we treat `created_date` as the firm's contract start date. However, in reality, many B2B SaaS customers begin with a trial or proof-of-concept, so their account creation date may differ from their paid contract start date. And this mismatch could create confusion in cohort analysis, which relies on the events data (e.g. first event timestamps). For example, if `created_date` (assumed as contract start date) is later than the firm's first usage event, the firm might appear to "churn" or "reactivate" incorrectly. To address this, the firms table should capture both the account creation date as well as the contract start date.
