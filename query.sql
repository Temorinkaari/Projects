WITH session_start AS
  (
  SELECT 
    user_pseudo_id,
    DATE_TRUNC(timestamp_micros(event_timestamp),day) AS session_day, 
    MIN(timestamp_micros(event_timestamp)) AS min_event_timestamp
  FROM `tc-da-1.turing_data_analytics.raw_events` 
  GROUP BY session_day, user_pseudo_id
  ),

session_end AS 
  (
  SELECT 
    user_pseudo_id,
    DATE_TRUNC(timestamp_micros(event_timestamp),day) AS session_day, 
    TIMESTAMP_ADD(MAX(timestamp_micros(event_timestamp)), INTERVAL 10 MINUTE) AS session_end, 
    category,
    country,
    campaign,
    CASE WHEN COUNT(user_pseudo_id) >=2 THEN 1 ELSE 0 END AS returning_customer
  FROM `tc-da-1.turing_data_analytics.raw_events` 
  WHERE campaign IS NOT NULL 
  GROUP BY user_pseudo_id,session_day, category, country, campaign
  ),

full_table AS 
  (
  SELECT 
    session_end.session_day, 
    session_end.user_pseudo_id,
    timestamp_diff(session_end.session_end, session_start.min_event_timestamp, second) AS duration_seconds,
    session_end.returning_customer,
    category,
    country,
    campaign
  FROM session_end
  INNER JOIN session_start
  ON session_start.user_pseudo_id = session_end.user_pseudo_id AND session_start.session_day = session_end.session_day
  )

SELECT 
  *,
  CASE WHEN returning_customer = 1 THEN duration_seconds END AS returning_customer_duration
FROM full_table
