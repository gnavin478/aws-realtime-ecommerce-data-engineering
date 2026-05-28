SELECT
    delivery_status,
    delivery_partner,
    state,
    SUM(total_delivery_events) AS total_delivery_events,
    AVG(average_delivery_charges) AS avg_delivery_charges
FROM delivery_summary
GROUP BY delivery_status, delivery_partner, state
ORDER BY total_delivery_events DESC;