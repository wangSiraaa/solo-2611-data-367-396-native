SELECT c.region, SUM(o.amount) AS total, COUNT(*) AS n FROM orders o LEFT JOIN customers c ON o.customer_id=c.customer_id WHERE o.status='ok' GROUP BY c.region;
