CREATE TABLE IF NOT EXISTS cx_history AS
WITH cte_app AS
(
	------Calculate number of applications and number of loans before
	SELECT  a.id, 
			a.created_at, 
			a.customer_id, 
			(COUNT(a.id) OVER (PARTITION BY a.customer_id ORDER BY a.created_at) - 1) number_of_app_before,
			GREATEST(0, (COUNT(a.loan_id) OVER (PARTITION BY a.customer_id ORDER BY a.created_at) - 1)) number_of_loans_before
	FROM 	applications a
), cte_paid AS
(
	SELECT  a.*, 
			b.paid_count, 
			b.notpaid_count
	FROM cte_app a
	LEFT JOIN (
		----------Number of paid and not paid cycles before
		SELECT  DISTINCT a.id, 
				a.created_at AS app_created, 
				a.customer_id,
				COUNT(
					CASE WHEN c.status = 'paid' THEN 1 ELSE NULL END
				) OVER (PARTITION BY c.customer_id ORDER BY a.created_at) paid_count,
				COUNT(
					CASE WHEN c.status <> 'paid' THEN 1 ELSE NULL END
				) OVER (PARTITION BY c.customer_id ORDER BY a.created_at) notpaid_count
		FROM	cycles c
		LEFT JOIN anyfin.applications a
		ON  c.customer_id = a.customer_id 
		 	AND c.created_at < a.created_at
	) b
	ON a.customer_id = b.customer_id
)
	SELECT  a.*, 
			b.avg_30_dpd, 
			b.max_30_dpd, 
			b.avg_60_dpd, 
			b.max_60_dpd
	FROM cte_paid a
	LEFT JOIN (
		SELECT  x.id, 
				x.app_created, 
				x.customer_id, 
				x.avg_30_dpd, 
				x.max_30_dpd, 
				y.avg_60_dpd, 
				y.max_60_dpd
		FROM
		  (
		  	-------------------Average and Maximum DPD before not more than 30 days
			SELECT  DISTINCT a.id, 
					a.created_at AS app_created, 
					a.customer_id,
					AVG(dpd) OVER (PARTITION BY c.customer_id ORDER BY a.created_at) avg_30_dpd,
					MAX(dpd) OVER (PARTITION BY c.customer_id ORDER BY a.created_at) max_30_dpd
			FROM	cycles c
			LEFT JOIN applications a
			ON  c.customer_id = a.customer_id 
				AND (c.created_at - INTERVAL '30 day') < a.created_at
		) x
		INNER JOIN (
			------------------Average and Maximum DPD before not more than 60 days
			SELECT  DISTINCT a.id, 
					a.created_at AS app_created, 
					a.customer_id,
					AVG(dpd) OVER (PARTITION BY c.customer_id ORDER BY a.created_at) avg_60_dpd,
					MAX(dpd) OVER (PARTITION BY c.customer_id ORDER BY a.created_at) max_60_dpd
			FROM	cycles c
			LEFT JOIN applications a
			ON 	c.customer_id = a.customer_id 
				AND (c.created_at - INTERVAL '60 day') < a.created_at 
		) y
		ON x.customer_id = y.customer_id
	) b
	ON a.customer_id = b.customer_id;