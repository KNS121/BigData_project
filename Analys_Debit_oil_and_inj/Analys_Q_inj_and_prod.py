import os
import sys

# Исправление пути для импорта
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config_to_connection
from DB_CONNECT import DatabaseConnector
import updatemissdata

class DataProcessor:
    def __init__(self, db_params):
        self.db_connector = DatabaseConnector(db_params)

    def connect(self):
        self.db_connector.connect()

    def create_result_table(self):
        query = """
    CREATE TABLE IF NOT EXISTS q_prod_and_inj (
        date_origin VARCHAR(7),
        field INTEGER,
        sum_q_prod FLOAT,
        sum_q_inj FLOAT);
    """
        self.db_connector.execute_query(query, fetch_results=False)

    def insert_data(self):
        query = """          
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
           
    """
    
        
    
        self.db_connector.execute_query(query, fetch_results=False)
        self.db_connector.commit()
    
    def up_miss(self):
        query_update_missing = updatemissdata.update_missing_data("q_prod_and_inj", 
                        "2015-12-01", "2017-04-01",
                        "2014-11-01", "2015-11-01",
                        "sum_q_inj", "sum_q_prod")
        self.db_connector.execute_query(query_update_missing, fetch_results=False)
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

def create_table_Q_prod_and_inj():
    db_params = config_to_connection.db_params
    data_proc = DataProcessor(db_params)
    data_proc.connect()
    data_proc.create_result_table()
    data_proc.insert_data()
    data_proc.up_miss()
    data_proc.close()

    print("Таблицы созданы и данные вставлены успешно.")
