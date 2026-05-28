SELECT
    SUM(total_revenue) AS total_revenue,
    SUM(total_orders) AS total_orders,
    SUM(total_quantity_sold) AS total_quantity_sold,
    AVG(average_order_value) AS avg_order_value
FROM sales_summary;