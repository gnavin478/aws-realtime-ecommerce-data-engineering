SELECT
    order_date,
    SUM(total_revenue) AS daily_revenue,
    SUM(total_orders) AS daily_orders
FROM sales_summary
GROUP BY order_date
ORDER BY order_date;