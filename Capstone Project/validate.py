import pandas as pd
import numpy as np
import configparser
import psycopg2
from sql_queries import table_names
from dbconnection import DBConnection
import logging
import logging_config

config = configparser.ConfigParser()
config.read_file(open('config.cfg'))

class Validate(DBConnection):
    """Validates data in AWS Redshift"""
    
    def __init__(self):
        super().__init__()
    
    def run_query(self, query: str) -> None:
        """Runs query and displays all the results"""
        
        self.cur.execute(query)
        rows = self.cur.fetchall()
        for row in rows:
            logging.info(row)
    
    def check_tables(self):
        """Displays what tables, columns and their types were created"""
        
        query = """SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{}';"""
        
        for table in table_names:
            self.cur.execute(query.format(table))
            rows = self.cur.fetchall()
            logging.info(f"\nThe table {table} has following columns and corresponding data types:")
            for row in rows:
                logging.info(f'{row[0]}: {row[1]}')
                
    def get_count(self) -> None:
        """Validates if tables are not empty"""
        
        query = "SELECT COUNT(*) FROM {};"
        
        for table in table_names:
            self.cur.execute(query.format(table))
            rows = self.cur.fetchall()
            for row in rows:
                if str(row[0])==0:
                    logging.info(f"The table {table} has is empty.")
                else:
                    logging.info(f"The count for the table {table} is: {row[0]}.")
                
    def check_for_nans(self):
        """Validates if tables contain NULLs"""
        
        query = """SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{}' AND is_nullable = 'YES'"""
        
        for table in table_names:
            self.cur.execute(query.format(table))
            rows = self.cur.fetchall()
            
            columns_null = False
            for row in rows:
                if row is None:
                    pass
                else:
                    columns_null = True
                    logging.info(f"Table {table} might contain NULL values.")
                    
            if not columns_null:
                logging.info(f"Table {table} does NOT contain NULL values.")
                
def main():
    """Pipline for validation of the data"""
    
    with Validate() as v:
        v.check_tables()
        v.get_count()
        v.check_for_nans()
        v.get_count()
        
if __name__ == "__main__":
    main()