SELECT  d.customer_id, 
        a.email
FROM(
    SELECT  customer_id
    FROM    cycles
    WHERE   dpd > 10
    GROUP BY customer_id
) d
LEFT JOIN applications a
ON d.customer_id = a.customer_id;