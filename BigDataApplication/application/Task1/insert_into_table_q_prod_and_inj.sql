CREATE TEMP TABLE Vrem_Table_1 AS
  SELECT DISTINCT
                TO_CHAR(TO_DATE(to_tdp.name_of_date_doc_col, 'YYYY-MM-DD'), 'YYYY-MM') AS date_origin,
                to_tdp.месторождение AS field,
                to_tdp.скважина AS well,
                COALESCE(NULLIF(to_tdp.факт_дебит_жидкости_м3_сут::text, 'NULL'), '0')::double precision AS q_prod,
                0 AS sum_q_inj
            FROM
                to_tdp
            WHERE
                to_tdp.факт_дебит_жидкости_м3_сут IS NOT NULL 
                AND to_tdp.факт_дебит_жидкости_м3_сут <> 'nan' 
                AND to_tdp.факт_дебит_жидкости_м3_сут <> '0.0'
                AND to_tdp.факт_дебит_жидкости_м3_сут <> '0'
            
            UNION
            
            SELECT DISTINCT
                TO_CHAR(TO_DATE(to_tdp_ppd.name_of_date_doc_col, 'YYYY-MM-DD'), 'YYYY-MM') AS date_origin,
                to_tdp_ppd.месторождение AS field,
                to_tdp_ppd.скважина AS well,
                0 AS q_prod,
                COALESCE(NULLIF(to_tdp_ppd.факт_закачка_за_месяц__м3::text, 'NULL'), '0')::double precision AS sum_q_inj
            FROM
                to_tdp_ppd
            WHERE
                to_tdp_ppd.факт_закачка_за_месяц__м3 IS NOT NULL 
                AND to_tdp_ppd.факт_закачка_за_месяц__м3 <> 'nan' 
                AND to_tdp_ppd.факт_закачка_за_месяц__м3 <> '0.0'
                AND to_tdp_ppd.факт_закачка_за_месяц__м3 <> '0';
                    
                    
    INSERT INTO q_prod_and_inj (date_origin, field, sum_q_prod, sum_q_inj)
    SELECT 
        Vrem_Table_1.date_origin AS date_origin,
        Vrem_Table_1.field AS field, 
        SUM(Vrem_Table_1.q_prod)*29.3 AS sum_q_prod, 
        SUM(Vrem_Table_1.sum_q_inj) AS sum_q_inj

    FROM
    Vrem_Table_1

    GROUP BY
    Vrem_Table_1.date_origin,
    Vrem_Table_1.field

    ORDER BY
    Vrem_Table_1.field,
    Vrem_Table_1.date_origin;