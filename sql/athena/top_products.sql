SELECT
    product_name,
    category,
    total_orders,
    total_quantity_sold,
    total_revenue
FROM top_products_analytics
ORDER BY total_revenue DESC
LIMIT 10;