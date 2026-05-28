SELECT
    activity_type,
    device_type,
    browser,
    state,
    COUNT(total_activities) AS total_activities,
    COUNT(total_sessions) AS total_sessions
FROM customer_behavior_analytics
GROUP BY
    activity_type,
    device_type,
    browser,
    state
ORDER BY total_activities DESC;