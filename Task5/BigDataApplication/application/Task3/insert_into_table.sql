CREATE TEMP TABLE Vrem_Table_1 AS
  SELECT DISTINCT
                TO_CHAR(TO_DATE(to_tdp.name_of_date_doc_col, 'YYYY-MM-DD'), 'YYYY-MM') AS date_origin,
                to_tdp.месторождение AS field,
                0 as sum_q_inj,
                COALESCE(NULLIF(to_tdp.факт_обводненность__perc_об::text, 'NULL'), '0')::double precision AS dsw,
                to_tdp.скважина as well_prod,
                '' as well_inj
            FROM
                to_tdp
            WHERE
                to_tdp.факт_обводненность__perc_об IS NOT NULL 
                AND to_tdp.факт_обводненность__perc_об <> 'nan' 
                AND to_tdp.факт_обводненность__perc_об <> '0.0'
                AND to_tdp.факт_обводненность__perc_об <> '0'
            
            UNION
            
            SELECT DISTINCT
                TO_CHAR(TO_DATE(to_tdp_ppd.name_of_date_doc_col, 'YYYY-MM-DD'), 'YYYY-MM') AS date_origin,
                to_tdp_ppd.месторождение AS field,
                COALESCE(NULLIF(to_tdp_ppd.факт_закачка_за_месяц__м3::text, 'NULL'), '0')::double precision AS sum_q_inj,
                0 as dsw,
                '' as well_prod,
                to_tdp_ppd.скважина as well_inj
            FROM
                to_tdp_ppd
            WHERE
                to_tdp_ppd.факт_закачка_за_месяц__м3 IS NOT NULL 
                AND to_tdp_ppd.факт_закачка_за_месяц__м3 <> 'nan' 
                AND to_tdp_ppd.факт_закачка_за_месяц__м3 <> '0.0'
               AND to_tdp_ppd.факт_закачка_за_месяц__м3 <> '0';
                    
                    
    INSERT INTO dsw_by_q_inj (date_origin, field, sum_q_inj, dsw)
    SELECT 
        Vrem_Table_1.date_origin AS date_origin,
        Vrem_Table_1.field AS field,  
        SUM(Vrem_Table_1.sum_q_inj) AS sum_q_inj,
        SUM(Vrem_Table_1.dsw)/COUNT(Vrem_Table_1.dsw) AS dsw

    FROM
    Vrem_Table_1

    GROUP BY
    Vrem_Table_1.field,
    Vrem_Table_1.date_origin

    ORDER BY
    Vrem_Table_1.field,
    Vrem_Table_1.date_origin;