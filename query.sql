WITH main_info AS 
(
  SELECT 
    user_pseudo_id,
    DATE_TRUNC(timestamp_micros(event_timestamp),day) AS session_day,
    timestamp_micros(event_timestamp) AS action_time,
    event_name,
    campaign,
    category,
    country
  FROM `tc-da-1.turing_data_analytics.raw_events` 
  WHERE campaign IS NOT NULL AND campaign IN ('referral', '<Other>', 'Data Share Promo', 'NewYear_V1', 'NewYear_V2', 'BlackFriday_V1', 'BlackFriday_V2', 'Holiday_V1', 'Holiday_V2') AND country <> '(not set)'
),

session_calculation AS 

(
  SELECT
    user_pseudo_id,
    session_day,
    action_time,
    ROW_NUMBER () OVER (win) AS rn,
    LAG(action_time) OVER (win) AS lag_,
    TIMESTAMP_DIFF(action_time, LAG(action_time) OVER (win), SECOND) AS duration,
    CASE WHEN TIMESTAMP_DIFF(action_time, LAG(action_time) OVER (win), SECOND) <= 600 THEN TIMESTAMP_DIFF(action_time, LAG(action_time) OVER (win), SECOND) END AS session_duration,
    campaign, 
    category,
    country
  FROM main_info
  WINDOW win AS (PARTITION BY user_pseudo_id ORDER BY action_time)
),

final_calculations AS

(  SELECT 
    user_pseudo_id,
    session_day,
    SUM(session_duration) AS total_session_duration,
    campaign,
    category,
    country,
    CASE WHEN COUNT(user_pseudo_id) >=2 THEN 1 ELSE 0 END AS returning_customer
  FROM session_calculation
  WHERE session_duration IS NOT NULL
  GROUP BY user_pseudo_id, session_day, campaign, category, country
)

SELECT *,
  CASE WHEN returning_customer = 1 THEN total_session_duration END AS returning_customer_duration
FROM final_calculations
