SELECT
    state,
    category,
    SUM(total_revenue) AS total_sales,
    SUM(total_orders) AS total_orders,
    SUM(total_quantity_sold) AS total_quantity_sold
FROM sales_summary
GROUP BY state, category
ORDER BY total_sales DESC;