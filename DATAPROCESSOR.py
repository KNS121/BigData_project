import os
import sys
from DB_CONNECT import DatabaseConnector
from updatemissdata import update_missing_data
from update_orient_column import update_orient_column

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class DataProcessor:
    def __init__(self, db_params, query_create_table, query_insert_data):
        self.db_connector = DatabaseConnector(db_params)
        self.query_create_table = query_create_table 
        self.query_insert_data = query_insert_data

    def connect(self):
        self.db_connector.connect()

    def create_result_table(self):
        self.db_connector.execute_query(self.query_create_table, fetch_results=False)

    def insert_data(self):
        self.db_connector.execute_query(self.query_insert_data, fetch_results=False)
        self.db_connector.commit()

    def up_miss(self, your_table, 
                date_miss_start, date_miss_end, 
                date_previous_start, date_previous_end, 
                column_miss, column_orient):
        query_update_missing = update_missing_data(your_table,
                        date_miss_start, date_miss_end,
                        date_previous_start, date_previous_end,
                        column_miss, column_orient)

        self.db_connector.execute_query(query_update_missing, fetch_results=False)
        self.db_connector.commit()

    # заполнение пропусков в колонке для ориентирования в методе up_miss
    def up_orient(self, your_table,
                date_miss_start, date_miss_end,
                date_previous_start, date_previous_end,
                column_orient):
        query_update_orient = update_orient_column(your_table,
                                                   date_miss_start, date_miss_end,
                                                   date_previous_start, date_previous_end,
                                                   column_orient)

        self.db_connector.execute_query(query_update_orient, fetch_results=False)
        self.db_connector.commit()

    def execute_query(self, query, fetch_results=True):
        self.db_connector.execute_query(query, fetch_results)
        self.db_connector.commit()

    def close(self):
        self.db_connector.close()