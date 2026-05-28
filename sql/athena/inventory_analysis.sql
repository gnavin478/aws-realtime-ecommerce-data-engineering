SELECT
    warehouse_name,
    product_name,
    stock_status,
    total_current_stock,
    average_current_stock
FROM inventory_summary
ORDER BY total_current_stock ASC;