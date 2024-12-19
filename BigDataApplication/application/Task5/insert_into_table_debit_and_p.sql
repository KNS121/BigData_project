CREATE temp TABLE if NOT EXISTS oil AS
    SELECT DISTINCT
        to_tdp."месторождение" AS field,
        to_tdp.name_of_date_doc_col AS date_origin,
        (COALESCE(NULLIF(to_tdp.факт_дебит_жидкости_м3_сут::TEXT, 'NULL'),'0')::DOUBLE PRECISION) * 29.3 AS q_prod,
        0 AS q_inj,
	    (COALESCE(NULLIF(to_tdp.факт_давление_пластовое_на_к::TEXT, 'NULL'),'0')::DOUBLE PRECISION) AS p,
	    (CASE WHEN to_tdp.факт_давление_пластовое_на_к <>'0'
	        AND to_tdp.факт_давление_пластовое_на_к <> 'NULL'
	        AND to_tdp.факт_давление_пластовое_на_к <> '0.0'
	    THEN 1 ELSE 0 END) AS cnt
    FROM to_tdp

	UNION

	SELECT DISTINCT
        to_tdp_ppd."месторождение",
        to_tdp_ppd.name_of_date_doc_col date,
	    0 AS q_prod,
        COALESCE(NULLIF(to_tdp_ppd.факт_закачка_за_месяц__м3::TEXT, 'NULL'),'0')::DOUBLE PRECISION AS q_inj,
        0 as p,
	    0 as cnt
    FROM to_tdp_ppd;

 
INSERT INTO debit_and_p (field, date_origin, sum_q_inj, sum_q_prod, diff_Q, avg_p_plast)
    SELECT DISTINCT
        oil.field::INTEGER field,
        TO_CHAR(TO_DATE(oil.date_origin, 'YYYY-MM-DD'), 'YYYY-MM') AS date_origin,
        SUM(oil.q_inj) AS sum_q_inj,
        SUM(oil.q_prod) AS sum_q_prod,
        SUM(oil.q_inj) - SUM(oil.q_prod) diff_q,
        SUM(oil.p) / (CASE WHEN SUM(oil.cnt) = 0 THEN 1 ELSE SUM(oil.cnt) END) avg_p_plast
    FROM oil
    GROUP BY oil.field, oil.date_origin
    ORDER BY field, date_origin;
  
