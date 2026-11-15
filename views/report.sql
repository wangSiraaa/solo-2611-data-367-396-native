SELECT region, CASE WHEN n > 1 THEN total ELSE 0 END AS displayed FROM customer_totals;
