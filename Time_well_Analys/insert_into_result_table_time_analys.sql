CREATE TEMP TABLE Vrem_Table_1 AS
  SELECT DISTINCT
                TO_CHAR(TO_DATE(to_tdp.name_of_date_doc_col, 'YYYY-MM-DD') - INTERVAL '1' MONTH, 'YYYY-MM') AS date_origin,
                to_tdp.месторождение AS field,
                to_tdp.скважина AS well_prod,
                '' AS well_inj,
                COALESCE(NULLIF(to_tdp.факт_время_работы__сут::text, 'NULL'), '0')::double precision AS days_prod,
                0 as days_inj
                
            FROM
                to_tdp
            WHERE
                to_tdp.факт_время_работы__сут IS NOT NULL 
                AND to_tdp.факт_время_работы__сут <> 'nan' 
                AND to_tdp.факт_время_работы__сут <> '0.0'
                AND to_tdp.факт_время_работы__сут <> '0'
              
            UNION
               
            SELECT DISTINCT
                TO_CHAR(TO_DATE(to_tdp_ppd.name_of_date_doc_col, 'YYYY-MM-DD') - INTERVAL '1' MONTH, 'YYYY-MM') AS date_origin,
                to_tdp_ppd.месторождение AS field,
                '' AS well_prod,
                to_tdp_ppd.скважина AS well_inj,
                0 AS days_prod,
                COALESCE(NULLIF(to_tdp_ppd.факт_время_работы___сут::text, 'NULL'), '0')::double precision AS days_inj
                
            FROM
                to_tdp_ppd
            WHERE
                to_tdp_ppd.факт_время_работы___сут IS NOT NULL 
                AND to_tdp_ppd.факт_время_работы___сут <> 'nan' 
                AND to_tdp_ppd.факт_время_работы___сут <> '0.0'
                AND to_tdp_ppd.факт_время_работы___сут <> '0';
                
    
    INSERT INTO avgtime (date_origin, field, avg_time_prod, avg_time_inj)
    SELECT 
        Vrem_Table_1.date_origin AS date_origin,
        Vrem_Table_1.field AS field, 
    
        SUM(Vrem_Table_1.days_prod)/COUNT(Vrem_Table_1.well_prod) AS avg_time_prod,
        SUM(Vrem_Table_1.days_inj)/COUNT(Vrem_Table_1.well_inj) AS avg_time_inj

    FROM
    Vrem_Table_1
    
    GROUP BY
    Vrem_Table_1.date_origin,
    Vrem_Table_1.field

    ORDER BY
    Vrem_Table_1.field,
    Vrem_Table_1.date_origin;