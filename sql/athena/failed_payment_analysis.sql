SELECT
    payment_method,
    payment_gateway,
    failure_reason,
    COUNT(*) AS total_failed_payments,
    SUM(payment_amount) AS failed_amount
FROM fact_payments
WHERE payment_status = 'FAILED'
GROUP BY
    payment_method,
    payment_gateway,
    failure_reason
ORDER BY total_failed_payments DESC;