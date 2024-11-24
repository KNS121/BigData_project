import os
import sys

# Исправление пути для импорта
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config_to_connection
from DB_CONNECT import DatabaseConnector

class DataProcessor:
    def __init__(self, db_params):
        self.db_connector = DatabaseConnector(db_params)

    def connect(self):
        self.db_connector.connect()

    def create_result_table(self):
        query = """
    CREATE TABLE IF NOT EXISTS Q_with_coords (
        date_origin VARCHAR(7),
        field INTEGER,
        well TEXT,
        sum_q_prod FLOAT,
        coordx FLOAT,
        coordy FLOAT
        
    );
    """
        self.db_connector.execute_query(query, fetch_results=False)

    def insert_data(self):
        query = """          
    CREATE TEMP TABLE Vrem_Table_1 AS
  SELECT DISTINCT
                TO_CHAR(TO_DATE(to_tdp.name_of_date_doc_col, 'YYYY-MM-DD'), 'YYYY-MM') AS date_origin,
                to_tdp.месторождение AS field,
                to_tdp.скважина AS well,
                COALESCE(NULLIF(to_tdp.факт_дебит_жидкости_м3_сут::text, 'NULL'), '0')::double precision AS Q_prod
                
            FROM
                to_tdp
            WHERE
                to_tdp.факт_дебит_жидкости_м3_сут IS NOT NULL 
                AND to_tdp.факт_дебит_жидкости_м3_сут <> 'nan' 
                AND to_tdp.факт_дебит_жидкости_м3_сут <> '0.0'
                AND to_tdp.факт_дебит_жидкости_м3_сут <> '0';

CREATE TEMP TABLE Vrem_Table_2 AS
        SELECT DISTINCT
    
                to_tdp.месторождение AS field,
                to_tdp.скважина AS well,
                MAX(COALESCE(NULLIF(to_tdp.coordx::text, 'NULL'), '0')::double precision) AS coordx,
                MAX(COALESCE(NULLIF(to_tdp.coordy::text, 'NULL'), '0')::double precision) AS coordy
       
            FROM
                to_tdp
                GROUP BY
                    to_tdp.месторождение,
                    to_tdp.скважина;
                    
                    
    INSERT INTO Q_with_coords (date_origin, field, well, sum_q_prod, coordx, coordy)
    SELECT 
    TO_CHAR(TO_DATE(Vrem_Table_1.date_origin,'YYYY-MM'), 'YYYY') AS date_origin,
    Vrem_Table_1.field AS field, 
    Vrem_Table_1.well AS well, 
    SUM(Vrem_Table_1.Q_prod)*29.3 AS sum_q_prod,
    Vrem_Table_2.coordx AS coordx,
    Vrem_Table_2.coordy AS coordy

    FROM
    Vrem_Table_1


    LEFT JOIN Vrem_Table_2 ON Vrem_Table_1.field = Vrem_Table_2.field AND
    Vrem_Table_1.well = Vrem_Table_2.well
    WHERE
    Vrem_Table_1.Q_prod <> 0

    GROUP BY
    TO_CHAR(TO_DATE(Vrem_Table_1.date_origin,'YYYY-MM'), 'YYYY'),
    Vrem_Table_1.field, 
    Vrem_Table_1.well, 
    Vrem_Table_2.coordx,
    Vrem_Table_2.coordy
    ORDER BY
    TO_CHAR(TO_DATE(Vrem_Table_1.date_origin,'YYYY-MM'), 'YYYY'),
    Vrem_Table_1.field;
           
    """
    
        self.db_connector.execute_query(query, fetch_results=False)
        self.db_connector.commit()
    
    def close(self):
        self.db_connector.close()

def create_table_Q_prod_and_inj():
    db_params = config_to_connection.db_params
    data_proc = DataProcessor(db_params)
    data_proc.connect()
    data_proc.create_result_table()
    data_proc.insert_data()
    data_proc.close()

    print("Таблицы созданы и данные вставлены успешно.")